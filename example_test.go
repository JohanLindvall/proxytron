package proxytron_test

import (
	"context"
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
