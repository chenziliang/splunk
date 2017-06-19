import subprocess


# total_events = 1000*1000*10
total_events = "10000"

test_cases = [
    # batch-size: 100, poll-limit 100
    {"sync-mode": "sync-mode", "concurrency": "1", "batch-size": "100", "poll-limit": "100", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "2", "batch-size": "100", "poll-limit": "100", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "4", "batch-size": "100", "poll-limit": "100", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "8", "batch-size": "100", "poll-limit": "100", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "16", "batch-size": "100", "poll-limit": "100", "poll-interval": "1s", "total-events": total_events},

    # batch-size: "100", poll-limit "1","000"
    {"sync-mode": "sync-mode", "concurrency": "1", "batch-size": "100", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "2", "batch-size": "100", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "4", "batch-size": "100", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "8", "batch-size": "100", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "16", "batch-size": "100", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},

    # batch-size: "1000", poll-limit "1","000"
    {"sync-mode": "sync-mode", "concurrency": "1", "batch-size": "1000", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "2", "batch-size": "1000", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "4", "batch-size": "1000", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "8", "batch-size": "1000", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "16", "batch-size": "1000", "poll-limit": "1000", "poll-interval": "1s", "total-events": total_events},

    # batch-size: "1000", poll-limit "10","000"
    {"sync-mode": "sync-mode", "concurrency": "1", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "2", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "4", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "8", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "sync-mode", "concurrency": "16", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},

    # batch-size: "100"
    {"sync-mode": "no-sync-mode", "concurrency": "1", "batch-size": "100", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "2", "batch-size": "100", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "4", "batch-size": "100", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "8", "batch-size": "100", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "16", "batch-size": "100", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},

    # batch-size: "1000"
    {"sync-mode": "no-sync-mode", "concurrency": "1", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "2", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "4", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "8", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
    {"sync-mode": "no-sync-mode", "concurrency": "16", "batch-size": "1000", "poll-limit": "10000", "poll-interval": "1s", "total-events": total_events},
]

class HecPerfDriver(object):

    def __init__(self, hec_host, sync_hec_token, async_hec_token):
        self.hec_host = hec_host
        self.sync_hec_token = sync_hec_token
        self.async_hec_token = async_hec_token

    def loop(self):
        for case in test_cases:
            if case["sync-mode"] == "sync-mode":
                token = self.sync_hec_token
            else:
                token = self.async_hec_token

            cmd = ["./http_event_perf", "--" + case["sync-mode"],
                   "--hec-uri", self.hec_host,
                   "--hec-token", token,
                   "--concurrency", case["concurrency"],
                   "--batch-size", case["batch-size"],
                   "--poll-limit", case["poll-limit"],
                   "--poll-interval", case["poll-interval"],
                   "--total-events", case["total-events"],
                   ]
            print " ".join(cmd)
            out, err = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print out
            print err
            print "----"

def main():
    hec_host = "https://54.183.23.176:8088"
    sync_hec_token = "F7EF6DB0-1D1F-40AD-B340-B110A0E2C373"
    async_hec_token = "F7EF6DB0-1D1F-40AD-B340-B110A0E2C374"
    driver = HecPerfDriver(hec_host, sync_hec_token, async_hec_token)
    driver.loop()


if __name__ == "__main__":
    main()
