import unittest
from airflow.models import DagBag

class TestGeneralDagIntegrity(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2
    def setUp(self):
        self.dagbag = DagBag()
        self.emails='airflow@example.com'

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_alert_email_present(self):
        for dag_id, dag in self.dagbag.dags.items():
            emails = dag.default_args.get('email', [])
            email_msg = 'Alert email not set for DAG {id}'.format(id=dag_id)
            self.assertIn(self.emails, emails, email_msg)

    def test_description_present(self):
        for dag_id, dag in self.dagbag.dags.items():
            descp_msg = 'Alert description not set for DAG {id}'.format(id=dag_id)
            self.assertNotEqual(dag.description, '', descp_msg)

suite = unittest.TestLoader().loadTestsFromTestCase(TestGeneralDagIntegrity)
unittest.TextTestRunner(verbosity=1).run(suite)