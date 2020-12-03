package unmanaged

import (
	"encoding/json"
	"io"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type identifierObject struct {
	Name    string `json:"name,omitempty"`
	Encoder string `json:"encoder,omitempty"`
}

type serializer struct {
	inner      runtime.Serializer
	identifier runtime.Identifier
}

func (s *serializer) Encode(obj runtime.Object, w io.Writer) error {
	if metaObj, ok := obj.(metav1.Object); ok {
		// TODO: should we copy the object?
		metaObj.SetManagedFields(nil)
	}
	return s.inner.Encode(obj, w)
}

func (s *serializer) Identifier() runtime.Identifier {
	return s.identifier
}

func (s *serializer) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	return s.inner.Decode(data, defaults, into)
}

var identifiersMap sync.Map

func identifier(encoder runtime.Encoder) runtime.Identifier {
	result := identifierObject{
		Name:    "unmanaged",
		Encoder: string(encoder.Identifier()),
	}
	if id, ok := identifiersMap.Load(result); ok {
		return id.(runtime.Identifier)
	}
	identifier, _ := json.Marshal(result)
	identifiersMap.Store(result, runtime.Identifier(identifier))
	return runtime.Identifier(identifier)
}

func NewSerializer(s runtime.Serializer) runtime.Serializer {
	if s == nil {
		return nil
	}
	return &serializer{
		inner:      s,
		identifier: identifier(s),
	}
}
