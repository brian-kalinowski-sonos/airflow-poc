import unittest
from datetime import datetime
from airflow.models import DagBag,TaskInstance

class TestDagCommiunication(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2
    def setUp(self):
        self.dagbag = DagBag()
        self.emails='airflow@example.com'
        self.dag_id = 'hello_world_xcoms'
        self.from_task ='push_to_xcoms'
        self.to_task1='pull_from_xcoms'
        self.to_task2='templated_xcoms_value'

    def test_xcoms(self):
        dag = self.dagbag.get_dag(self.dag_id)
        push_to_xcoms_task = dag.get_task(self.from_task)
        pull_from_xcoms_task = dag.get_task(self.to_task1)
        execution_date = datetime.now()
        push_to_xcoms_ti = TaskInstance(task=push_to_xcoms_task, execution_date=execution_date)
        context = push_to_xcoms_ti.get_template_context()
        push_to_xcoms_task.execute(context)
        pull_from_xcoms_ti = TaskInstance(task=pull_from_xcoms_task, execution_date=execution_date)
        result = pull_from_xcoms_ti.xcom_pull(key="dummyKey")
        self.assertEqual(result, 'dummyValue')

    def test_xcom_in_templated_field(self):

        dag = self.dagbag.get_dag(self.dag_id)
        push_to_xcoms_task = dag.get_task(self.from_task)
        execution_date = datetime.now()
        push_to_xcoms_ti = TaskInstance(task=push_to_xcoms_task, execution_date=execution_date)
        context = push_to_xcoms_ti.get_template_context()
        push_to_xcoms_task.execute(context)
        templated_xcoms_value_task = dag.get_task(self.to_task2)
        templated_xcoms_value_ti = TaskInstance(task=templated_xcoms_value_task, execution_date=execution_date)
        context = templated_xcoms_value_ti.get_template_context()
        bash_operator_templated_field = 'bash_command'
        rendered_template = templated_xcoms_value_task.render_template
        bash_command_value = getattr(templated_xcoms_value_task, bash_operator_templated_field)
        bash_command_rendered_value = rendered_template(bash_command_value,context)
        self.assertEqual(bash_command_rendered_value, 'echo dummyValue')

suite = unittest.TestLoader().loadTestsFromTestCase(TestDagCommiunication)
unittest.TextTestRunner(verbosity=2).run(suite)