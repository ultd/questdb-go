package questdb

type option struct {
	tableName string
}

// WithTableName func should allow you to set a model's table name for different client operations
func WithTableName(tableName string) option {
	return option{
		tableName: tableName,
	}
}
