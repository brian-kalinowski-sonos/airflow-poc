import argparse
import unittest
from airflow.models import DagBag
import sys

class TestWorkflow(unittest.TestCase):
    """Check SA expectation"""

    def __init__(self,test_name,dagbag,dag_id,tt,task_id,upstream_tasks,downstream_tasks):
        super(TestWorkflow, self).__init__(test_name)
        self.dagbag=dagbag
        self.dag_id=dag_id
        self.tt=tt
        self.task_id=task_id
        self.upstream_tasks=upstream_tasks
        self.downstream_tasks=downstream_tasks

    def test_task_count(self):
        """Check task count of hello_world dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), self.tt)

    def test_upstream_dependency(self):
        param_task = self.dagbag.get_dag(self.dag_id).get_task(self.task_id)
        upstream_task_ids = list(map(lambda task: task.task_id, param_task.upstream_list))
        self.assertCountEqual(upstream_task_ids, self.upstream_tasks)

    def test_downstream_dependency(self):
        param_task = self.dagbag.get_dag(self.dag_id).get_task(self.task_id)
        downstream_task_ids = list(map(lambda task: task.task_id, param_task.downstream_list))
        self.assertCountEqual(downstream_task_ids, self.downstream_tasks)


if __name__ == '__main__':
    dagbag = DagBag()
    parser = argparse.ArgumentParser("DAG Definition testing\n")
    parser.add_argument("dag_id",help="Id of the DAG whose defn is to be tested.")
    parser.add_argument("-tt", "--total-tasks", type=int)
    parser.add_argument("-ut", "--upstream-tasks",action='append',help="Upstream task_ids of the task_id parsed",default=[])
    parser.add_argument("-dt","--downstream-tasks",action='append', help="Downstream task_ids of the task_id parsed", default=[])
    parser.add_argument("-t","--task-id", help=" task_id to be tested. ",default='')
    args=parser.parse_args()

    test_loader = unittest.TestLoader()
    test_names = test_loader.getTestCaseNames(TestWorkflow)

    suite = unittest.TestSuite()

    for test_name in test_names:
        suite.addTest(TestWorkflow(test_name,dagbag,args.dag_id,args.total_tasks,args.task_id,args.upstream_tasks,args.downstream_tasks))
    result = unittest.TextTestRunner().run(suite)
    sys.exit(not result.wasSuccessful())

