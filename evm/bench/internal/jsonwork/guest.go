package jsonwork

func Build(seed int) any {
	rows := make([]any, 128)
	for i := range rows {
		rows[i] = map[string]any{
			"id":      int64(i + 1),
			"token":   uint64(1<<63) + uint64(i),
			"active":  (i+seed)%5 != 0,
			"name":    "customer",
			"balance": float64(100+i) + 0.25 + float64(seed)/8,
			"labels":  []any{"member", true, nil},
			"profile": map[string]any{"score": int64(i % 17), "credit": float64(i)/2 + 0.25},
			"note":    nil,
		}
	}
	return map[string]any{"rows": rows, "version": int64(seed), "source": "import"}
}

func Read(doc any) int64 {
	root := doc.(map[string]any)
	total := root["version"].(int64) + int64(len(root["source"].(string)))
	for _, value := range root["rows"].([]any) {
		row := value.(map[string]any)
		if !row["active"].(bool) {
			continue
		}
		profile := row["profile"].(map[string]any)
		labels := row["labels"].([]any)
		total += row["id"].(int64) + int64(row["token"].(uint64)-uint64(1<<63))
		total += int64(row["balance"].(float64)*4) + profile["score"].(int64)
		total += int64(profile["credit"].(float64)*2) + int64(len(row["name"].(string)))
		total += int64(len(labels[0].(string)) + len(labels))
		if labels[1].(bool) && labels[2] == nil && row["note"] == nil {
			total += 7
		}
	}
	return total
}

func Update(doc any, seed int) int64 {
	root := doc.(map[string]any)
	root["version"] = int64(seed)
	for i, value := range root["rows"].([]any) {
		row := value.(map[string]any)
		row["active"] = (i+seed)%5 != 0
		row["balance"] = float64(100+i) + 0.25 + float64(seed)/8
		name := "member"
		if (i+seed)&1 != 0 {
			name = "preferred"
		}
		row["name"] = name
		profile := row["profile"].(map[string]any)
		profile["score"] = int64((i + seed) % 17)
		labels := row["labels"].([]any)
		labels[0] = name
		row["labels"] = labels[:3:3]
	}
	return Read(doc)
}

func BuildRead(seed int) int64 { return Read(Build(seed)) }

func Rebox(n int) int64 {
	var number any = int64(0)
	var text any = "customer"
	for i := 0; i < n; i++ {
		number = number.(int64) + int64(i)
		text = text.(string)
	}
	return number.(int64) + int64(len(text.(string)))
}
