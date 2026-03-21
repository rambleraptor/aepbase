// Package cel2sql converts CEL filter expressions to SQL WHERE clauses.
// It is adapted from github.com/aep-dev/aepc/pkg/cel2ansisql with fixes
// for cel-go operator naming conventions (e.g. "_==_" instead of "==").
package cel2sql

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Convert compiles a CEL expression against the given environment and returns
// the equivalent SQL WHERE clause.
func Convert(env *cel.Env, filter string) (string, error) {
	ast, issues := env.Compile(filter)
	if issues != nil && issues.Err() != nil {
		return "", fmt.Errorf("compiling filter: %w", issues.Err())
	}
	checked, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return "", err
	}
	return convertExpr(checked.Expr)
}

func convertExpr(expr *exprpb.Expr) (string, error) {
	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		return convertCall(expr.GetCallExpr())
	case *exprpb.Expr_IdentExpr:
		return expr.GetIdentExpr().Name, nil
	case *exprpb.Expr_ConstExpr:
		return handleConst(expr.GetConstExpr())
	default:
		return "", fmt.Errorf("unsupported expression type: %T", expr.ExprKind)
	}
}

func convertCall(call *exprpb.Expr_Call) (string, error) {
	if call.Target != nil {
		return convertCallWithTarget(call.Function, call.Target, call.Args)
	}
	if len(call.Args) == 1 {
		return handleUnaryOp(call.Function, call.Args[0])
	}
	if len(call.Args) == 2 {
		return handleBinaryOp(call.Function, call.Args[0], call.Args[1])
	}
	return "", fmt.Errorf("unsupported call: %s with %d args", call.Function, len(call.Args))
}

func convertCallWithTarget(function string, target *exprpb.Expr, args []*exprpb.Expr) (string, error) {
	targetSQL, err := convertExpr(target)
	if err != nil {
		return "", err
	}
	if len(args) != 1 {
		return "", fmt.Errorf("unsupported target call %s with %d args", function, len(args))
	}
	argSQL, err := convertExpr(args[0])
	if err != nil {
		return "", err
	}
	switch function {
	case "startsWith":
		return fmt.Sprintf("%s LIKE CONCAT(%s, '%%')", targetSQL, argSQL), nil
	case "contains":
		return fmt.Sprintf("%s LIKE CONCAT('%%', %s, '%%')", targetSQL, argSQL), nil
	case "endsWith":
		return fmt.Sprintf("%s LIKE CONCAT('%%', %s)", targetSQL, argSQL), nil
	default:
		return "", fmt.Errorf("unsupported function: %s", function)
	}
}

func handleUnaryOp(function string, arg *exprpb.Expr) (string, error) {
	sql, err := convertExpr(arg)
	if err != nil {
		return "", err
	}
	switch function {
	case "!_", "!":
		return fmt.Sprintf("NOT (%s)", sql), nil
	case "-_", "-":
		return fmt.Sprintf("-%s", sql), nil
	default:
		return "", fmt.Errorf("unsupported unary operator: %s", function)
	}
}

func handleBinaryOp(function string, left, right *exprpb.Expr) (string, error) {
	leftSQL, err := convertExpr(left)
	if err != nil {
		return "", err
	}
	rightSQL, err := convertExpr(right)
	if err != nil {
		return "", err
	}

	// cel-go wraps binary operators with underscores: _==_, _!=_, etc.
	var op string
	switch function {
	case "_==_":
		op = "="
	case "_!=_":
		op = "<>"
	case "_<_":
		op = "<"
	case "_<=_":
		op = "<="
	case "_>_":
		op = ">"
	case "_>=_":
		op = ">="
	case "_&&_":
		op = "AND"
	case "_||_":
		op = "OR"
	default:
		return "", fmt.Errorf("unsupported operator: %s", function)
	}
	return fmt.Sprintf("%s %s %s", leftSQL, op, rightSQL), nil
}

func handleConst(c *exprpb.Constant) (string, error) {
	switch c.ConstantKind.(type) {
	case *exprpb.Constant_NullValue:
		return "NULL", nil
	case *exprpb.Constant_StringValue:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(c.GetStringValue(), "'", "''")), nil
	case *exprpb.Constant_BoolValue:
		if c.GetBoolValue() {
			return "TRUE", nil
		}
		return "FALSE", nil
	case *exprpb.Constant_Int64Value:
		return fmt.Sprintf("%d", c.GetInt64Value()), nil
	case *exprpb.Constant_DoubleValue:
		return fmt.Sprintf("%f", c.GetDoubleValue()), nil
	default:
		return "", fmt.Errorf("unsupported constant type: %T", c.ConstantKind)
	}
}
