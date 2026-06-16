module github.com/redis/rueidis/rueidishook

go 1.25.0

replace (
	github.com/redis/rueidis => ../
	github.com/redis/rueidis/mock => ../mock
)

require (
	github.com/redis/rueidis v1.0.73
	github.com/redis/rueidis/mock v1.0.73
	go.uber.org/mock v0.6.0
)

require golang.org/x/sys v0.46.0 // indirect
