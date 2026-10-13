package lease

import "time"

const (
	defaultRefresh             = 24 * time.Hour
	defaultRefreshRetry        = 5 * time.Minute
	defaultTick                = time.Minute
	defaultFeedTimeout         = 20 * time.Second
	defaultFeedBytes           = 32 << 20
	defaultMaxEntries          = 100000
	defaultMaxFeeds            = 32
	defaultPageSize            = 50
	defaultMaxPageSize         = 200
	defaultDBTimeout           = time.Second
	defaultRetireAfter         = 30 * 24 * time.Hour
	defaultRefusalBase         = time.Minute
	defaultRefusalMax          = 30 * time.Minute
	defaultBrokenCooldown      = 10 * time.Minute
	defaultDeadCooldown        = 24 * time.Hour
	defaultOutcomeDecay        = 0.9
	defaultDurationAlpha       = 0.3
	defaultPriorValid          = 0.25
	defaultPriorSeconds        = 8.0
	defaultPriorWeight         = 4.0
	defaultPriorStrength       = 2.0
	defaultInvalidPenalty      = 10.0
	defaultMinSeconds          = 0.05
	defaultThroughputMinBytes  = 64 << 10
	defaultPriorBytesPerSecond = 1 << 20
	defaultPriorSetup          = 30 * time.Second
	defaultScoreBytes          = 32 << 20
	defaultSampleWindow        = 5 * time.Second
	defaultExploreInterval     = 30 * time.Second
	defaultUpgradeWarmup       = 20 * time.Second
	defaultUpgradeInterval     = 10 * time.Second
	defaultUpgradeMinGain      = 15 * time.Second
	defaultUpgradeMinBytes     = 4 << 20
	defaultUpgradeRatio        = 1.25
	defaultUpgradeConfirm      = 2

	// defaultDrawTries is how many routes are drawn at random, looking for an
	// unmeasured one that could take a transfer, before listing them all.
	defaultDrawTries = 1024
	defaultExitTTL   = time.Hour
)
