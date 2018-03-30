from functools import reduce
from copy import deepcopy


class PyungoError(Exception):
    pass


def topological_sort(data):
    for key in data:
        data[key] = set(data[key])
    for k, v in data.items():
        v.discard(k)  # ignore self dependencies
    extra_items_in_deps = reduce(set.union, data.values()) - set(data.keys())
    data.update({item: set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item,dep in data.items() if not dep)
        if not ordered:
            break
        yield sorted(ordered)
        data = {item: (dep - ordered) for item,dep in data.items()
                if item not in ordered}
    if data:
        raise PyungoError('A cyclic dependency exists amongst {}'.format(data))


class Node(object):
    ID = 0
    def __init__(self, fct, input_names, output_names, args=None, kwargs=None, map_=None, aggregate=None):
        Node.ID += 1
        self._id = str(Node.ID)
        self._fct = fct
        self._input_names = input_names
        self._args = args if args else []
        self._kwargs = kwargs if kwargs else []
        self._output_names = output_names
        self._map = map_
        self._aggregate = aggregate

    def __repr__(self):
        return '{}({}, <{}>, {}, {})'.format(
            self.__class__.__name__,
            self._id, self._fct.__name__,
            self._input_names, self._output_names
        )

    def __call__(self, args, **kwargs):
        return self._fct(*args, **kwargs)

    @property
    def id(self):
        return self._id

    @property
    def input_names(self):
        input_names = self._input_names
        input_names.extend(self._args)
        input_names.extend(self._kwargs)
        return input_names

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def output_names(self):
        return self._output_names

    @property
    def fct_name(self):
        return self._fct.__name__


class DynamicNode(Node):
    pass


class MapNode(DynamicNode):

    @classmethod
    def from_node(cls, node, input_names_map, i):
        node = deepcopy(node)
        input_names = node._input_names
        output_names = node._output_names
        new_input_names = []
        for inp in input_names:
            if inp in input_names_map:
                new_input_names.append(inp+'_'+str(i))
            else:
                new_input_names.append(inp)
        output_names = [out+'_'+str(i) for out in output_names]
        return cls(node._fct, new_input_names, output_names, node._args, node._kwargs)


class AggregationNode(DynamicNode):
    def __init__(self, *args, **kwargs):
        super(AggregationNode, self).__init__(*args, **kwargs)
        self._fct = AggregationNode.aggregate

    @staticmethod
    def aggregate(*args):
        return args


class Graph:
    def __init__(self):
        self._nodes = []
        self._dynamic_nodes = None
        self._data = None

    @property
    def data(self):
        return self._data

    @property
    def sim_inputs(self):
        inputs = []
        for node in self._nodes:
            inputs.extend(node.input_names)
        return inputs

    @property
    def sim_outputs(self):
        outputs = []
        for node in self._nodes:
            outputs.extend(node.output_names)
        return outputs

    @property
    def dag(self):
        """ return the ordered nodes graph """
        nodes = self._nodes  # FIXME: this is a hack
        if self._dynamic_nodes:
            self._nodes = self._dynamic_nodes
        ordered_nodes = []
        for node_ids in topological_sort(self._dependencies()):
            nodes = [self._get_node(node_id) for node_id in node_ids]
            ordered_nodes.append(nodes)
        self._nodes = nodes
        return ordered_nodes

    def _register(self, f, **kwargs):
        input_names = kwargs.get('inputs')
        args_names = kwargs.get('args')
        kwargs_names = kwargs.get('kwargs')
        output_names = kwargs.get('outputs')
        map_ = kwargs.get('map')
        aggregate = kwargs.get('aggregate')
        self._create_node(
            f, input_names, output_names, args_names, kwargs_names, map_, aggregate
        )

    def register(self, **kwargs):
        def decorator(f):
            self._register(f, **kwargs)
            return f
        return decorator

    def add_node(self, function, **kwargs):
        self._register(function, **kwargs)

    def _create_node(self, fct, input_names, output_names, args_names, kwargs_names, map_, aggregate):
        node = Node(fct, input_names, output_names, args_names, kwargs_names, map_, aggregate)
        # assume that we cannot have two nodes with the same output names
        for n in self._nodes:
            for out_name in n.output_names:
                if out_name in node.output_names:
                    msg = '{} output already exist'.format(out_name)
                    raise PyungoError(msg)
        self._nodes.append(node)

    def _dependencies(self):
        dep = {}
        for node in self._nodes:
            if node._map:
                continue
            d = dep.setdefault(node.id, [])
            for inp in node.input_names:
                for node2 in self._nodes:
                    if node2._map:
                        continue
                    if inp in node2.output_names:
                        d.append(node2.id)
        return dep

    def _get_node(self, id_):
        for node in self._nodes:
            if node.id == id_:
                return node

    def _check_inputs(self, data):
        data_inputs = set(data.keys())
        diff = data_inputs - (data_inputs - set(self.sim_outputs))
        if diff:
            msg = 'The following inputs are already used in the model: {}'.format(list(diff))
            raise PyungoError(msg)
        inputs_to_provide = set(self.sim_inputs) - set(self.sim_outputs)
        diff = inputs_to_provide - data_inputs
        if diff:
            msg = 'The following inputs are needed: {}'.format(list(diff))
            raise PyungoError(msg)
        diff = data_inputs - inputs_to_provide
        if diff:
            msg = 'The following inputs are not used by the model: {}'.format(list(diff))
            raise PyungoError(msg)

    def _create_dynamic_nodes(self, data):
        new_nodes = []
        map_len = None  # we allow a single mapping
        for node in self._nodes:
            # TODO: do we need the below?
            if isinstance(node, DynamicNode):
                continue
            if node._map:
                # iterrate over each map input_name
                for input_name in node._map:
                    if input_name in data:  # TODO: raise error if input name SHOULD be in data (first node)
                        # we remove the input
                        values = data.pop(input_name)
                        # save / check map_len
                        new_map_len = len(values)
                        if not map_len:
                            map_len = new_map_len
                        else:
                            if new_map_len != map_len:
                                raise NotImplementedError('Map length should be the same in all nodes of the graph')
                        try:
                            iter(values)
                        except TypeError:
                            raise PyungoError('{} should be iterrable'.format(input_name))
                        for j, value in enumerate(values):
                            # transorm the input name
                            data[input_name+'_'+str(j)] = value
                for i in range(len(values)):  # FIXME: hacky
                    new_node = MapNode.from_node(node, node._map, i)
                    new_nodes.append(new_node)
            if node._aggregate:
                if not map_len:
                    raise PyungoError('Aggregate can be used only with a map')
                for agg_name in node._aggregate:
                    agg_node = AggregationNode(None, [agg_name+'_'+str(i) for i in range(map_len)], agg_name)
                    new_nodes.append(agg_node)

        self._nodes.extend(new_nodes)

    def _delete_dynamic_nodes(self):
        self._dynamic_nodes_snapshot()
        self._nodes = [node for node in self._nodes if not isinstance(node, DynamicNode)]

    def _dynamic_nodes_snapshot(self):
        self._dynamic_nodes = deepcopy(self._nodes)

    def calculate(self, data):
        self._data = deepcopy(data)
        self._check_inputs(data)
        self._create_dynamic_nodes(self._data)
        dep = self._dependencies()
        sorted_dep = topological_sort(dep)
        for items in sorted_dep:
            for item in items:
                node = self._get_node(item)
                args = [i_name for i_name in node.input_names if i_name not in node.kwargs]
                data_to_pass = []
                for arg in args:
                    data_to_pass.append(self._data[arg])
                kwargs_to_pass = {}
                for kwarg in node.kwargs:
                    kwargs_to_pass[kwarg] = self._data[kwarg]
                res = node(data_to_pass, **kwargs_to_pass)
                if len(node.output_names) == 1:
                    self._data[node.output_names[0]] = res
                else:
                    for i, out in enumerate(node.output_names):
                        self._data[out] = res[i]
        self._delete_dynamic_nodes()
        return res
