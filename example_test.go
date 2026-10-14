package proxytron_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func ExamplePool() {
	pool := proxytron.FromURLs([]string{"http://proxy.example.test:8080"})
	defer pool.Close()
	route := pool.Acquire("catalog", false, nil, nil)
	// Send the request using route, then classify the response in the application.
	pool.ReportOK(context.Background(), "catalog", route, 200*time.Millisecond)
	fmt.Println(pool.SiteHealth("catalog")[0].Served)
	// Output: 1
}

func ExampleParseFeed() {
	entries, err := proxytron.ParseFeed([]byte(`[
	  {"proxy":"socks5://proxy.example.test:1080","geolocation":{"country":"SE"}},
	  "direct"
	]`), "http", "se")
	if err != nil {
		panic(err)
	}
	fmt.Println(entries[0].URL)
	// Output: socks5h://proxy.example.test:1080
}

func ExampleRedact() {
	fmt.Println(proxytron.Redact("http://user:secret@proxy.example.test:8080"))
	// Output: http://***@proxy.example.test:8080
}

func ExamplePool_Do() {
	pool := proxytron.FromURLs([]string{"http://first.example.test:8080", "http://second.example.test:8080"})
	defer pool.Close()
	attempts := 0
	var body string
	err := pool.Do(context.Background(), "catalog", proxytron.RequestOptions{Attempts: 1},
		func(ctx context.Context, endpoint string) proxytron.AttemptResult {
			attempts++
			// An application sends and classifies the request here. These
			// synthetic results demonstrate the two independent retry budgets.
			if endpoint == "http://first.example.test:8080" {
				return proxytron.AttemptResult{Outcome: proxytron.TransportFailed,
					Err: errors.New("connection failed"), Retry: true, RouteFailure: true}
			}
			body = "response data"
			return proxytron.AttemptResult{Outcome: proxytron.Succeeded}
		})
	fmt.Println(body, attempts, err)
	// Output: response data 2 <nil>
}

func ExamplePacer() {
	pacer := proxytron.NewPacer(proxytron.PacerConfig{MinInterval: time.Second}, nil)
	httpRoute := pacer.ForRoute("http://proxy.example.test:8080")
	socksRoute := pacer.ForRoute("socks5h://proxy.example.test:1080")
	fmt.Println(httpRoute == socksRoute)
	// Call httpRoute.Wait(ctx) before sending and Refused or OK afterward.
	// Output: true
}
