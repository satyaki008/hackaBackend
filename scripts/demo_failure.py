"""Standalone Terminal Demo: Distributed Node Failure & Self-Healing Recovery."""
import sys
import time
import httpx

def main():
    print("=" * 75)
    print("  AEGISSTORE: LIVE DISTRIBUTED NODE FAILURE & SELF-HEALING DEMO")
    print("=" * 75)
    print("  Connecting to AegisStore Controller at http://127.0.0.1:8000...")

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post("http://127.0.0.1:8000/api/demo/auto-repair")
            if resp.status_code != 200:
                print(f"  [ERROR] Server returned error: {resp.text}")
                sys.exit(1)

            data = resp.json()
            timeline = data.get("timeline", [])

            for item in timeline:
                step = item.get("step")
                title = item.get("title")
                status = item.get("status")
                details = item.get("details", "")

                status_tag = f"[{status}]"
                print(f"\n  STEP {step:02d} | {status_tag:<12} | {title}")
                if details:
                    if isinstance(details, dict):
                        for k, v in details.items():
                            print(f"          * {k}: {v}")
                    else:
                        print(f"          > {details}")
                time.sleep(0.3)

            print("\n" + "=" * 75)
            if data.get("success"):
                print("  DEMO RESULT: [SUCCESS] System restored full replication factor.")
                print(f"  Object ID:   {data.get('object_id')}")
            else:
                print("  DEMO RESULT: [FAILED] See details above.")
            print("=" * 75)

    except httpx.ConnectError:
        print("\n  [ERROR] Could not connect to AegisStore at http://127.0.0.1:8000.")
        print("  Please start the cluster first using: python scripts/run_system.py")
        sys.exit(1)

if __name__ == "__main__":
    main()
