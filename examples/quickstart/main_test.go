package main

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

// Example keeps the README's quickstart output exercised by the normal Go suite.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, &output); err != nil {
		panic(err)
	}
	fmt.Print(output.String())
	// Output:
	// proxy A: hello from the local destination
	// proxy B: hello from the local destination
	// proxy A: hello from the local destination
	// proxy B: hello from the local destination
	// Recorded successes: 4
}
