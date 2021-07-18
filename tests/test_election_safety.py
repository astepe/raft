from main.raft import RaftCluster


class TestElectionSafety:

    @staticmethod
    def test_one_leader():
        """
        Test that only 1 leader exists for each term.
        """
        raft_cluster = RaftCluster()
        raft_cluster.start()
        leader_ids = set()
        for server in raft_cluster.servers:
            leader_ids.add(server.leader_id)

        assert len(leader_ids) == 1
        raft_cluster.stop()
