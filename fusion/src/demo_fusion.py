"""Run from the project root: python -m src.demo_fusion."""

import simpy

from src.fusion import FusionCluster


def demo(env, cluster):
    for primary, value in enumerate((10, 20, 30)):
        yield env.process(cluster.put(primary, "balance", value))
    print("Primary maps:", {p: n.data for p, n in cluster.primaries.items()})
    print("Fused XOR backup:", cluster.backup.parity)
    cluster.crash(1)
    print("Primary 1 crashed; its memory is now:", cluster.primaries[1].data)
    yield env.process(cluster.put(0, "balance", 15))
    yield env.process(cluster.recover(1))
    print("Primary 1 recovered:", cluster.primaries[1].data)
    yield env.process(cluster.put(1, "balance", 25))
    value = yield env.process(cluster.get(1, "balance"))
    print("Primary 1 continues writing; balance:", value)
    cluster.crash("backup")
    yield env.process(cluster.recover("backup"))
    print("Backup rebuilt:", cluster.backup.parity)


if __name__ == "__main__":
    environment = simpy.Environment()
    cluster = FusionCluster(environment)
    environment.run(until=environment.process(demo(environment, cluster)))
