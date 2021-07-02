package table

type columns = map[string]struct{}

type Table struct {
	name    string
	columns columns
}

func New(name string, columns columns) Table {
	return Table{
		name:    name,
		columns: columns,
	}
}

func (t Table) Name() string {
	return t.name
}

func (t Table) Columns() columns {
	return t.columns
}

func (t Table) ListColumns() []string {
	out := []string{}
	for name := range t.columns {
		out = append(out, name)
	}
	return out
}

func (t Table) OmitColumns(omits ...string) []string {
	omit := columns{}
	for _, o := range omits {
		if _, ok := t.columns[o]; ok {
			omit[o] = struct{}{}
		}
	}

	out := []string{}
	for name := range t.columns {
		if _, ok := omit[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}
