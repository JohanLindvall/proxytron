package lease

var fixturePolicy = Policy{RecoveryScopes: []string{"cloudflare"}, CappedScopes: []string{"keep2share", "fileboom"}}

const testHealthEpoch = 2

func testPolicy() Option {
	return func(p *Pool) { WithPolicy(fixturePolicy)(p); WithHealthEpoch(testHealthEpoch)(p) }
}
