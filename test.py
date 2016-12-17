import bmemcached

def run_test():
    client = bmemcached.Client(servers="localhost:9090")
    client.set("sunil", "arora")
    val = client.get("sunil")
    assert val == "arora"

if __name__ == "__main__":
    run_test()
