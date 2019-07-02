

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
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            if self.templates_dict:
                self.op_kwargs['templates_dict'] = self.templates_dict
            # generate filenames
            input_filename = os.path.join(tmp_dir, 'script.in')
            output_filename = os.path.join(tmp_dir, 'script.out')
            string_args_filename = os.path.join(tmp_dir, 'string_args.txt')
            script_filename = os.path.join(tmp_dir, 'script.py')

            # set up virtualenv
            self._execute_in_subprocess(self._generate_virtualenv_cmd(tmp_dir))
            cmd = self._generate_pip_install_cmd(tmp_dir)
            if cmd:
                self._execute_in_subprocess(cmd)

            self._write_args(input_filename)
            self._write_script(script_filename)
            self._write_string_args(string_args_filename)

            # execute command in virtualenv
            self._execute_in_subprocess(
                self._generate_python_cmd(tmp_dir,
                                          script_filename,
                                          input_filename,
                                          output_filename,
                                          string_args_filename))
            return self._read_result(output_filename)

    def _pass_op_args(self):
        # we should only pass op_args if any are given to us
        return len(self.op_args) + len(self.op_kwargs) > 0

    def _execute_in_subprocess(self, cmd):
        try:
            self.log.info("Executing cmd\n%s", cmd)
            output = subprocess.check_output(cmd,
                                             stderr=subprocess.STDOUT,
                                             close_fds=True)
            if output:
                self.log.info("Got output\n%s", output)
        except subprocess.CalledProcessError as e:
            self.log.info("Got error output\n%s", e.output)
            raise

    def _write_string_args(self, filename):
        # writes string_args to a file, which are read line by line
        with open(filename, 'w') as file:
            file.write('\n'.join(map(str, self.string_args)))

    def _write_args(self, input_filename):
        # serialize args to file
        if self._pass_op_args():
            with open(input_filename, 'wb') as file:
                arg_dict = ({'args': self.op_args, 'kwargs': self.op_kwargs})
                if self.use_dill:
                    dill.dump(arg_dict, file)
                else:
                    pickle.dump(arg_dict, file)

    def _read_result(self, output_filename):
        if os.stat(output_filename).st_size == 0:
            return None
        with open(output_filename, 'rb') as file:
            try:
                if self.use_dill:
                    return dill.load(file)
                else:
                    return pickle.load(file)
            except ValueError:
                self.log.error("Error deserializing result. "
                               "Note that result deserialization "
                               "is not supported across major Python versions.")
                raise

    def _write_script(self, script_filename):
        with open(script_filename, 'w') as file:
            python_code = self._generate_python_code()
            self.log.debug('Writing code to file\n', python_code)
            file.write(python_code)

    def _generate_virtualenv_cmd(self, tmp_dir):
        cmd = ['virtualenv', tmp_dir]
        if self.system_site_packages:
            cmd.append('--system-site-packages')
        if self.python_version is not None:
            cmd.append('--python=python{}'.format(self.python_version))
        return cmd

    def _generate_pip_install_cmd(self, tmp_dir):
        if len(self.requirements) == 0:
            return []
        else:
            # direct path alleviates need to activate
            cmd = ['{}/bin/pip'.format(tmp_dir), 'install']
            return cmd + self.requirements

    @staticmethod
    def _generate_python_cmd(tmp_dir, script_filename,
                             input_filename, output_filename, string_args_filename):
        # direct path alleviates need to activate
        return ['{}/bin/python'.format(tmp_dir), script_filename,
                input_filename, output_filename, string_args_filename]

    def _generate_python_code(self):
        if self.use_dill:
            pickling_library = 'dill'
        else:
            pickling_library = 'pickle'
        fn = self.python_callable
        # dont try to read pickle if we didnt pass anything
        if self._pass_op_args():
            load_args_line = 'with open(sys.argv[1], "rb") as file: arg_dict = {}.load(file)'\
                .format(pickling_library)
        else:
            load_args_line = 'arg_dict = {"args": [], "kwargs": {}}'

        # no indents in original code so we can accept
        # any type of indents in the original function
        # we deserialize args, call function, serialize result if necessary
        return dedent("""\
        import {pickling_library}
        import sys
        {load_args_code}
        args = arg_dict["args"]
        kwargs = arg_dict["kwargs"]
        with open(sys.argv[3], 'r') as file:
            virtualenv_string_args = list(map(lambda x: x.strip(), list(file)))
        {python_callable_lines}
        res = {python_callable_name}(*args, **kwargs)
        with open(sys.argv[2], 'wb') as file:
            res is not None and {pickling_library}.dump(res, file)
        """).format(load_args_code=load_args_line,
                    python_callable_lines=dedent(inspect.getsource(fn)),
                    python_callable_name=fn.__name__,
                    pickling_library=pickling_library)
