module github.com/teamgram/marmota

go 1.16

require (
	github.com/Shopify/sarama v1.30.0
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/zeromicro/go-zero v1.3.0
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/go-playground/validator.v9 v9.31.0
)

replace github.com/zeromicro/go-zero v1.3.0 => github.com/teamgramio/go-zero v1.3.1-0.20220221143929-c2897dcaf14c
