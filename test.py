import bmemcached

def run_test():
    client = bmemcached.Client(servers="localhost:9090")
    client.set("sunil", "arora")
    val = client.get("sunil")
    assert val == "arora"
    client.set("a", 5)
    val = client.get("a")
    assert val == 5

if __name__ == "__main__":
    run_test()
