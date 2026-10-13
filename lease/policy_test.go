package lease

var fixturePolicy = Policy{RecoveryScopes: []string{"cloudflare"}, CappedScopes: []string{"keep2share", "fileboom"}}

func testPolicy() Option { return WithPolicy(fixturePolicy) }
