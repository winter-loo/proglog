package auth

import (
	"fmt"
	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func New(model, policy string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, err
	}
	return &Authorizer{
		enforcer: enforcer,
	}, nil
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if err := a.enforcer.LoadPolicy(); err != nil {
		return status.Errorf(codes.Internal, "failed to load policy: %v", err)
	}
	allowed, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to enforce policy: %v", err)
	}
	if !allowed {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		return status.Error(codes.PermissionDenied, msg)
	}
	return nil
}
