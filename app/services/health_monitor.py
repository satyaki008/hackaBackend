"""Background Health Monitoring & Failure Detector Service.
Polls storage nodes with periodic heartbeats, measures round-trip response times, detects node crashes, and triggers immediate self-healing repairs.
"""
import asyncio
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.services.repair import repair_manager
from app.services.integrity import integrity_checker
from app.services.rebalancer import storage_rebalancer
from app.config import settings

logger = logging.getLogger("health_monitor")

class HealthMonitorService:
    def __init__(self):
        self.is_running = False
        self._monitor_task: asyncio.Task = None
        self._integrity_task: asyncio.Task = None
        self._rebalance_task: asyncio.Task = None

    async def start(self):
        """Start background monitoring tasks."""
        if self.is_running:
            return
        self.is_running = True
        self._monitor_task = asyncio.create_task(self._heartbeat_loop())
        self._integrity_task = asyncio.create_task(self._integrity_loop())
        self._rebalance_task = asyncio.create_task(self._rebalance_loop())
        logger.info("Health Monitor, Integrity Auditor, and Rebalancer background tasks started.")

    async def stop(self):
        """Gracefully stop background tasks."""
        self.is_running = False
        for task in [self._monitor_task, self._integrity_task, self._rebalance_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("Health Monitor stopped.")

    async def _heartbeat_loop(self):
        """Continuous heartbeat poll loop."""
        while self.is_running:
            try:
                db: Session = SessionLocal()
                try:
                    nodes = metadata_service.list_nodes(db)
                    for node in nodes:
                        is_healthy, payload, latency_ms = await storage_node_client.get_health(node.url)
                        
                        if is_healthy:
                            was_offline = node.status in ("OFFLINE", "RECOVERING")
                            node.status = "ONLINE"
                            node.used_capacity_bytes = payload.get("used_bytes", 0)
                            node.free_capacity_bytes = payload.get("free_bytes", settings.DEFAULT_NODE_CAPACITY_BYTES)
                            node.object_count = payload.get("object_count", 0)
                            node.simulated_latency_ms = payload.get("simulated_latency_ms", 0)
                            node.is_partitioned = payload.get("is_partitioned", False)
                            node.response_time_ms = round(latency_ms, 2)
                            node.last_heartbeat = datetime.now(timezone.utc)
                            db.commit()

                            if was_offline:
                                metadata_service.log_event(
                                    db,
                                    level="SUCCESS",
                                    component="FAILURE_DETECTOR",
                                    message=f"Storage node {node.id} has recovered and is now ONLINE (Ping: {node.response_time_ms}ms)."
                                )
                                # Re-check if any degraded objects can now be restored
                                asyncio.create_task(repair_manager.scan_and_repair_all(db))
                        else:
                            was_online = node.status == "ONLINE"
                            node.status = "OFFLINE"
                            node.response_time_ms = round(latency_ms, 2)
                            db.commit()

                            if was_online:
                                metadata_service.log_event(
                                    db,
                                    level="WARNING",
                                    component="FAILURE_DETECTOR",
                                    message=f"Storage node {node.id} heartbeat failed / unreachable! Triggering self-healing repair."
                                )
                                # Trigger immediate automatic self-healing repair for this node's objects
                                asyncio.create_task(repair_manager.handle_node_failure(db, node.id))
                finally:
                    db.close()
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                
            await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SECONDS)

    async def _integrity_loop(self):
        """Periodic background integrity verification."""
        await asyncio.sleep(10)  # Initial grace period on boot
        while self.is_running:
            try:
                db: Session = SessionLocal()
                try:
                    await integrity_checker.run_audit(db, auto_repair=True)
                finally:
                    db.close()
            except Exception as e:
                logger.error(f"Error in background integrity loop: {e}")
                
            await asyncio.sleep(settings.INTEGRITY_CHECK_INTERVAL_SECONDS)

    async def _rebalance_loop(self):
        """Periodic background rebalance inspection."""
        await asyncio.sleep(25)  # Initial delay
        while self.is_running:
            try:
                db: Session = SessionLocal()
                try:
                    analysis = storage_rebalancer.analyze_balance(db)
                    if not analysis["is_balanced"] and analysis["recommended_moves"]:
                        metadata_service.log_event(
                            db,
                            level="INFO",
                            component="REBALANCER",
                            message=f"Storage imbalance detected (skew {analysis['skew_percentage']}%). Running background rebalance."
                        )
                        await storage_rebalancer.execute_rebalance(db, max_migrations=2)
                finally:
                    db.close()
            except Exception as e:
                logger.error(f"Error in background rebalance loop: {e}")

            await asyncio.sleep(settings.REBALANCE_INTERVAL_SECONDS)

health_monitor = HealthMonitorService()
