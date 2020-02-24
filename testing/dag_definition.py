import unittest
from airflow.models import DagBag

class TestNeonWorkflow(unittest.TestCase):
    """Check SA expectation"""

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'neon_workflow'

    def test_task_count(self):
        """Check task count of hello_world dag"""
        # dag_id='hello_world'
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 8)

    def test_contain_tasks(self):
        """Check task contains in hello_world dag"""
        dag_id='hello_world'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['start', 'end','sales-captureData','travel-captureData',\
                                        'sales-cleanseData','travel-cleanseData','travel-sinkData',\
                                        'sales-sinkData'])

    def test_dependencies_of_dummy_task(self):
        """Check the task dependencies of dummy_task in hello_world dag"""
        dag_id='hello_world'
        dag = self.dagbag.get_dag(dag_id)
        dummy_task = dag.get_task('dummy_task')

        upstream_task_ids = list(map(lambda task: task.task_id, dummy_task.upstream_list))
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, dummy_task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['hello_task', 'multiplyby5_task'])

    def test_dependencies_of_hello_task(self):
        """Check the task dependencies of hello_task in hello_world dag"""
        dag_id='hello_world'
        dag = self.dagbag.get_dag(dag_id)
        hello_task = dag.get_task('hello_task')

        upstream_task_ids = list(map(lambda task: task.task_id, hello_task.upstream_list))
        self.assertListEqual(upstream_task_ids, ['dummy_task'])
        downstream_task_ids = list(map(lambda task: task.task_id, hello_task.downstream_list))
        self.assertListEqual(downstream_task_ids, [])

suite = unittest.TestLoader().loadTestsFromTestCase(TestNeonWorkflow)
unittest.TextTestRunner(verbosity=2).run(suite)