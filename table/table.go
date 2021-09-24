package table

type columns = map[string]struct{}

type Table struct {
	Name    string `json:"name"`
	Columns columns
}

func New(name string, columns columns) Table {
	return Table{
		Name:    name,
		Columns: columns,
	}
}

func (t Table) GetName() string {
	return t.Name
}

func (t Table) GetColumns() columns {
	return t.Columns
}

func (t Table) ListColumns() []string {
	out := []string{}
	for name := range t.Columns {
		out = append(out, name)
	}
	return out
}

func (t Table) OmitColumns(omits ...string) []string {
	omit := columns{}
	for _, o := range omits {
		if _, ok := t.Columns[o]; ok {
			omit[o] = struct{}{}
		}
	}

	out := []string{}
	for name := range t.Columns {
		if _, ok := omit[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}
