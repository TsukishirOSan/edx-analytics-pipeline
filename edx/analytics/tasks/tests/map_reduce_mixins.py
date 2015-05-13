"""Utility mixins that simplify tests for map reduce jobs."""
import json

import luigi


class MapperTestMixin(object):
    """Base class for map function tests"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    task_class = None

    def setUp(self):
        self.event_templates = {}
        self.default_event_template = ''
        self.create_task()

    def create_task(self, interval=None):
        """Allow arguments to be passed to the task constructor."""
        if not interval:
            interval = self.DEFAULT_DATE
        self.task = self.task_class(
            interval=luigi.DateIntervalParameter().parse(interval),
            output_root='/fake/output',
        )
        self.task.init_local()

    def create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        template_name = kwargs.get('template_name', self.default_event_template)
        event_dict = kwargs.pop('template', self.event_templates[template_name]).copy()
        event_dict.update(**kwargs)
        return event_dict

    def assert_single_map_output(self, line, expected_key, expected_value):
        """Assert that an input line generates exactly one output record with the expected key and value"""
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        actual_key, actual_value = row
        self.assertEquals(expected_key, actual_key)
        self.assertEquals(expected_value, actual_value)

    def assert_no_map_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(
            tuple(self.task.mapper(line)),
            tuple()
        )
