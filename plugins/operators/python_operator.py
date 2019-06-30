

import inspect
import os
import pickle
import subprocess
import sys
import types
from textwrap import dedent
from typing import Optional, Iterable, Dict, Callable

import dill

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class PythonOperator(BaseOperator):

    template_fields = ('templates_dict', 'op_args', 'op_kwargs', '.py')
    ui_color = '#ffefeb'

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs = ('python_callable', 'op_kwargs',)

    @apply_defaults
    def __init__(
        self,
        python_callable,
        op_args: Optional[Iterable] = None,
        op_kwargs: Optional[Dict] = None,
        provide_context: bool = False,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[Iterable[str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context):
        # Export context to make it available for callables to use.
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info("Exporting the following env vars:\n%s",
                      '\n'.join(["{}={}".format(k, v)
                                 for k, v in airflow_context_vars.items()]))
        os.environ.update(airflow_context_vars)

        if self.provide_context:
            context.update(self.op_kwargs)
            context['templates_dict'] = self.templates_dict
            self.op_kwargs = context

        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self):
        return self.python_callable(*self.op_args, **self.op_kwargs)
