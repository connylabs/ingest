package config

import (
	"encoding/json"
	"errors"
	"time"
)

// Thank you stackoverflow
// https://stackoverflow.com/questions/48050945/how-to-unmarshal-json-into-durations
// We need to make our own Duration type that implements UnmarshalJSON
// to be able to unmarshal time.Duration.

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}
