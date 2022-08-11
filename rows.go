package astra

import (
	"fmt"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

type colSpec struct {
	names []string
	idxs  map[string]int
}

// Row represents a row of data from an Astra table.
type Row struct {
	spec   *colSpec
	values []any
}

// Scan copies the values from the row into the provided pointers.
func (r *Row) Scan(dest ...any) error {
	if len(r.values) != len(dest) {
		return fmt.Errorf("row has %d values, got %d pointers", len(r.values), len(dest))
	}
	for i, v := range r.values {
		if err := convertAssign(dest[i], v); err != nil {
			return fmt.Errorf("failed to assign in row %q at index %d: %w", r, i, err)
		}
	}
	return nil
}

// Values returns the values in the row.
func (r *Row) Values() []any {
	return r.values
}

// String returns a string representation of the values in the row.
func (r *Row) String() string {
	return fmt.Sprintf("%v", r.values)
}

// Rows represents a list of Astra table rows.
type Rows []Row

func newRowsFromResultSet(rs *pb.ResultSet) (Rows, error) {
	var cs *colSpec
	if cols := rs.Columns; cols != nil {
		cs = &colSpec{
			names: make([]string, len(cols)),
			idxs:  make(map[string]int, len(cols)),
		}
		for i, col := range cols {
			cs.names[i] = col.Name
			cs.idxs[col.Name] = i
		}
	}

	res := make(Rows, len(rs.Rows))
	for i, row := range rs.Rows {
		vs, err := protosToValue(row.Values, rs.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row %q at index %d: %w", row, i, err)
		}
		res[i] = Row{spec: cs, values: vs}
	}
	return res, nil
}
