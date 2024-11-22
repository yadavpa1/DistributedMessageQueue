import sys

def generate_docker_compose(instances):
    compose_file = """version: '1'

services:
"""
    for i in range(1, instances + 1):
        compose_file += f"""
    zoo{i}:
        image: manios/zookeeper:3.4.5
        hostname: zoo{i}
        container_name: zoo{i}
        restart: always
        ports:
            - "{2180+i}:2181"    # Client port for node {i}
            - "{2880+i}:2888"    # Quorum port for node {i}
            - "{3880+i}:3888"    # Leader election port for node {i}
        environment:
            ZOO_MY_ID: {i}
            ZOO_SERVERS: {" ".join([f"server.{j}=zoo{j}:2888:3888" for j in range(1, instances + 1)])}
        volumes:
            - ./zookeeper_data/data{i}:/data
            - ./zookeeper_data/datalog{i}:/datalog
            - ./zookeeper_data/config{i}:/conf
"""
    return compose_file

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_compose.py <number_of_instances>")
        sys.exit(1)

    try:
        num_instances = int(sys.argv[1])
        if num_instances <= 0:
            raise ValueError("Number of instances must be a positive integer.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        sys.exit(1)

    docker_compose_content = generate_docker_compose(num_instances)

    with open("docker-compose.yml", "w") as compose_file:
        compose_file.write(docker_compose_content)

    print(f"Docker Compose file for {num_instances} Zookeeper instances generated successfully!")
