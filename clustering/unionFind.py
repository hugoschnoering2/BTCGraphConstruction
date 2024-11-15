
from typing import Optional


class UnionFind(object):

    def __init__(self, ids: Optional[list] = None):

        self.cluster_root = {i: i for i in ids} if ids is not None else {}
        self.cluster_rank = {i: 0 for i in ids} if ids is not None else {}

    def add(self, new_ids: list):
        for i in new_ids:
            if i not in self.cluster_root:
                self.cluster_root[i] = i
                self.cluster_rank[i] = 0

    def find(self, i):
        nodes = []
        x = i
        while x != self.cluster_root[x]:
            nodes.append(x)
            x = self.cluster_root[x]
        for node in nodes:
            self.cluster_root[node] = x
        return x

    def union(self, i, j) -> None:

        root_i = self.find(i)
        root_j = self.find(j)

        if root_i == root_j:
            return None

        if self.cluster_rank[root_i] < self.cluster_rank[root_j]:
            self.cluster_root[root_i] = root_j
        elif self.cluster_rank[root_i] > self.cluster_rank[root_j]:
            self.cluster_root[root_j] = root_i
        else:
            self.cluster_root[root_j] = root_i
            self.cluster_rank[root_i] += 1

    @property
    def num_ids(self) -> int:
        return len(self.cluster_root)

    @property
    def clusters_ids(self) -> set:
        clusters = set()
        for i in self.cluster_root.keys():
            root = self.find(i)
            clusters.add(root)
        return clusters
