package dequeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

// Transfer copies the given item from the client to the storage
// unless the storage already contains it.
// It returns the URL of the newly stored object or nil if the object already existed.
func Transfer(ctx context.Context, c ingest.Client, s storage.Storage, item ingest.Codec) (*url.URL, error) {
	_, err := s.Stat(ctx, item)
	if err == nil {
		return nil, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	obj, err := c.Download(ctx, item)
	if err != nil {
		return nil, err
	}
	return s.Store(ctx, item, *obj)
}

// CallWebhook sends the given URIs as a JSON array to the webhook URL.
func CallWebhook(ctx context.Context, webhookURL string, uris []string) error {
	requestData, err := json.Marshal(uris)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewBuffer(requestData))
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	defer io.Copy(io.Discard, res.Body) //nolint:errcheck

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status code: %d", res.StatusCode)
	}

	return nil
}
