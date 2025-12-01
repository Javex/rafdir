package internal

import (
	"encoding/json"
	"fmt"
	"time"
)

type Duration struct {
	time.Duration
}

func (duration *Duration) UnmarshalJSON(b []byte) error {
	// From https://biscuit.ninja/posts/go-unmarshalling-json-into-time-duration/
	var unmarshalledJson any

	if err := json.Unmarshal(b, &unmarshalledJson); err != nil {
		return err
	}

	switch value := unmarshalledJson.(type) {
	case float64:
		duration.Duration = time.Duration(value)
	case string:
		if parsedDuration, err := time.ParseDuration(value); err != nil {
			return err
		} else {
			duration.Duration = parsedDuration
		}
	default:
		return fmt.Errorf("invalid duration: %#v", unmarshalledJson)
	}

	return nil
}
