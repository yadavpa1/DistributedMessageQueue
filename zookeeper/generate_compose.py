import sys

def generate_docker_compose(zk_instances, bk_instances):
    compose_file = """version: '3'

networks:
    zk-bookie-net:
        name: zk-bookie-net
        driver: bridge

services:
"""
    for i in range(1, zk_instances + 1):
        compose_file += f"""
    zoo{i}:
        image: zookeeper:3.8.0
        hostname: zoo{i}
        container_name: zoo{i}
        networks:
            - zk-bookie-net
        ports:
            - "{2180+i}:2181"    # Client port for node {i}
            - "{9080+i}:8080"    # Admin Server port for node {i}
        environment:
            - ZOO_MY_ID={i}
            - ZOO_SERVERS={" ".join([f"server.{j}=zoo{j}:2888:3888;2181" for j in range(1, zk_instances + 1)])}
            - ZOO_STANDALONE_ENABLED=false
        healthcheck:
            test: ["CMD", "curl", "-s", "http://localhost:8080/commands/stat"]
            interval: 60s
            timeout: 3s
            retries: 60
        restart: on-failure
"""
    for i in range(1, bk_instances + 1):
        compose_file += f"""
    bookie{i}:
        image: apache/bookkeeper:4.15.3
        hostname: bookie{i}
        container_name: bookie{i}
        networks:
            - zk-bookie-net
        ports:
            - "{3180+i}:3181"    # Client port for node {i}
            - "{4180+i}:4181"    # Admin Server port for node {i}
            - "{8080+i}:8080"    # HTTP Server port for node {i}
        environment:
            - BK_zkServers={",".join([f"zoo{j}:2181" for j in range(1, zk_instances + 1)])}
            - BK_metadataServiceUri=zk+hierarchical://{";".join([f"zoo{j}:2181" for j in range(1, zk_instances + 1)])}/ledgers
            - BK_DATA_DIR=/data/bookkeeper
            - BK_advertisedAddress=127.0.0.1
            - BK_bookiePort={3180+i}
            - BK_httpServerEnabled=true
        depends_on:"""
        for j in range(1, zk_instances + 1):
            compose_file += f"""
            - zoo{j}"""

        compose_file += f"""
        healthcheck:
            test: ["CMD", "curl", "-s", "http://localhost:8080/health"]
            interval: 60s
            timeout: 3s
            retries: 60
        restart: on-failure
        """
    return compose_file

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_compose.py <number_of_zk_instances> <number_of_bk_instances>")
        sys.exit(1)

    try:
        zk_instances = int(sys.argv[1])
        if zk_instances <= 0:
            raise ValueError("Number of instances must be a positive integer.")
        bk_instances = int(sys.argv[2])
        if bk_instances <= 0:
            raise ValueError("Number of instances must be a positive integer.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        sys.exit(1)

    docker_compose_content = generate_docker_compose(zk_instances, bk_instances)

    with open("docker-compose.yml", "w") as compose_file:
        compose_file.write(docker_compose_content)

    print(f"Docker Compose file for {zk_instances} Zookeeper instances and {bk_instances} Bookkeeper instances generated successfully!")