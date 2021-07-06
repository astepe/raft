from main.raft import RaftCluster


class TestLeaderElection:

    @staticmethod
    def test_leader_elected():
        raft_cluster = RaftCluster()