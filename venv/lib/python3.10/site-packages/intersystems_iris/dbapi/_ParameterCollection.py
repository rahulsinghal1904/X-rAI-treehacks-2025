import intersystems_iris.dbapi._ResultSetRow
from intersystems_iris.dbapi._Parameter import _Parameter
from intersystems_iris.dbapi._Parameter import ParameterMode

class _ParameterCollection:
    def __init__(self, param_coll = None, shallow_copy = False):
        self._params_list = list() # contains elements of type _Parameter
        self._has_bound_by_param_name = False

        self._user_index = list()
        self._user_param_cnt = 0

        self._param_names = None
        self._array_bound = False

        self._input_params = list()

        if param_coll != None:
            if shallow_copy:
                self._params_list = param_coll._params_list
            else:
                self._params_list = list()
                for param in param_coll._params_list:
                    new_param = _Parameter()
                    new_param._copy_cached_info(param, True)
                    self._params_list.append(new_param)

            self._user_param_cnt = param_coll._user_param_cnt
            self._user_index = param_coll._user_index
            
            if param_coll._has_named_parameters():
                self._update_names()
            self._has_bound_by_param_name = False
            self._array_bound = False

    def set_input_params(self, params):
        self._input_params = list(params)

    def collect(self, i: int = 0) -> list:
        params = list()
                
        param_index = 0
        param_counter = i
        for param in self._params_list:
            mode = param.mode
            if mode == ParameterMode.REPLACED_LITERAL:
                params.append(_Parameter(param.value).value)
            elif not mode == ParameterMode.INPUT:
                params.append(_Parameter(param._values[i]).value)
            elif self._input_params:
                item = self._input_params[param_counter]
                if isinstance(item, tuple) or isinstance(item, list):
                    value = item[param_index]
                    param_index = param_index + 1
                else:
                    value = item
                    param_counter = param_counter + 1
                params.append(_Parameter(value).value)
            else:
                raise Exception("Missing value")

        return params

    def update(self, set_index, param_index, value):
        if isinstance(self._input_params[set_index], tuple):
            self._input_params[set_index] = list(self._input_params[set_index])
        if isinstance(self._input_params[set_index], list):
            self._input_params[set_index][param_index] = value
        else:
            self._input_params[param_index] = value

    def __repr__(self) -> str:
        return repr(self.collect())

    def _add_user_param(self, param):
        self._user_param_cnt = self._user_param_cnt + 1
        #self._params_list.append(param)
        return

    def _user_parameters_size(self):
        if self._user_index == None:
            return len(self._params_list)
        return self._user_param_cnt

    def _has_named_parameters(self):
        return self._param_names != None and len(self._params_list) != 0
    
    def _update_names(self):
        self._param_names = {}
        for i in range(len(self._params_list)):
            key = self._params_list[i].name.upper()
            self._param_names[key] = i
        return

    def _get_user_param_index(self, user_parameter_number):
        actual_index = self._user_index[user_parameter_number]
        if actual_index == -1:
            raise IndexError("Invalid parameter number: " + str(user_parameter_number))
        return actual_index

    def _get_user_param(self, user_parameter_number):
        if user_parameter_number >= len(self._user_index):
            raise IndexError("Invalid parameter number: " + str(user_parameter_number))
        actual_index = self._get_user_param_index(user_parameter_number)
        return self._params_list[actual_index]

    def _update_param_info(self, from_param_coll):
        try:
            for i, param in enumerate(from_param_coll._params_list):
                self._params_list[i]._copy_cached_info(param, False)

            if self._user_param_cnt != len(from_param_coll._params_list):
                self._user_param_cnt = from_param_coll._user_param_cnt
                self._user_index = from_param_coll._user_index

            if from_param_coll._has_named_parameters():
                self._update_names()

        except Exception:
            # need to handle exceptions properly
            if len(from_param_coll._params_list) != len(self._params_list):
                raise LookupError("Parameter mismatch")
            raise Exception

    def _clear(self):
        self._params_list.clear()
        if self._param_names != None:
            self._param_names.clear()
        self._has_bound_by_param_name = False
        self._array_bound = False

    def _prep_list_index(self, is_fast_select, output_parameter_list):
        self._param_row = intersystems_iris.dbapi._ResultSetRow._ResultSetRow(rowcount = len(self._params_list))
        self._param_row._fast_select = is_fast_select
        self._param_row.indexRow(output_parameter_list.list_item)
        return

    def _get_user_list_offset(self, user_parameter_number):
        if user_parameter_number >= len(self._user_index):
            raise IndexError("Invalid parameter number: " + str(user_parameter_number))
        return self._param_row.rowItems[self._get_user_param_index(user_parameter_number)]