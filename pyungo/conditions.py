import ast
import operator as op


OPERATORS = {ast.Gt: op.gt,
             ast.Lt: op.lt,
             ast.GtE: op.ge,
             ast.LtE: op.le,
             ast.Eq: op.eq,
             ast.NotEq: op.ne}


def is_true(node, data):
    if isinstance(node, ast.Compare):
        # keep things simple for now
        if len(node.ops) > 1 or len(node.comparators) > 1:
            msg = 'Multiple comparison not implemented'
            raise NotImplementedError(msg)
        if isinstance(node.left, ast.Name):
            val = data[node.left.id]
            node.left = ast.Num(val)
            return is_true(node, data)
        if isinstance(node.comparators[0], ast.Name):
            val = data[node.comparators[0].id]
            node.comparators[0] = ast.Num(val)
            return is_true(node, data)
        operator = OPERATORS.get(node.ops[0].__class__)
        if not operator:
            msg = 'Operator not implemented'
            raise NotImplementedError(msg)
        return operator(
            node.left.n,
            node.comparators[0].n
        )


def evaluate(expression, data):
    return is_true(ast.parse(expression, mode='eval').body, data)
