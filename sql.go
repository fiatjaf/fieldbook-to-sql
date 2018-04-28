package main

import (
	"io/ioutil"
	"strings"

	"github.com/gosimple/slug"
	"github.com/jmoiron/sqlx"
	"github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func buildsqlite(bookPath string, dbPath string) error {
	f, _ := ioutil.ReadFile(bookPath)
	var model Model
	err := json.Unmarshal(f, &model)
	if err != nil {
		return err
	}

	db, err := sqlx.Connect("sqlite3", dbPath)
	if err != nil {
		return err
	}

	// var indexes []string
	// var foreignkeys []string

	log.Debug().Int("count", len(model.Sheets)).Msg("will insert sheets")
	for sidx, sheet := range model.Sheets {
		var fields []Field
		var fieldNames []string

		for fidx, field := range sheet.Fields {
			name := field.Key
			typ := "text"

			if field.Name != "" {
				name = strings.Replace(slug.Make(field.Name), "-", "_", -1)
			}
			if name == "__name__" {
				continue
			}

			switch field.Type {
			case "enum":
				// log.Print("enum: ", field.Enum)
				typ = "text"
			case "formula":
				log.Print("formula: ", field.Expression)
				continue
			case "join":
				if name == field.Key {
					var joinedSheet string
					for _, join := range model.Joins {
						log.Print("join search ", join)
						if join.Left.SheetId == sheet.Id &&
							join.Left.FieldKey == field.Key {
							joinedSheet = join.Right.SheetId
							log.Print("found ", joinedSheet)
							break
						}
						if join.Right.SheetId == sheet.Id &&
							join.Right.FieldKey == field.Key {
							joinedSheet = join.Left.SheetId
							log.Print("found ", joinedSheet)
							break
						}
					}
					for _, sheet := range model.Sheets {
						if sheet.Id == joinedSheet {
							name = sheet.Title
							break
						}
					}
				}
			case "generic":
				log.Debug().Str("name", field.Name).Msg("field")

				// investigating actual type
				var actualType string
				for _, rec := range sheet.Records {
					if val, ok := rec[field.Key]; ok {
						if iv, ok := val.(map[string]interface{}); ok {
							if thisType, ok := iv["type"].(string); ok {
								if actualType == "" {
									actualType = thisType
								} else if actualType != thisType {
									// mismatch, let's use 'text'
									log.Debug().Str("act", actualType).
										Str("thi", thisType).Msg("mismatch")
								}
							}
						}
					}
				}
				log.Print("actual type: " + actualType)
				model.Sheets[sidx].Fields[fidx].ActualType = actualType
				switch actualType {
				case "string", "image", "dayofyear":
					typ = "text"
				case "numeric", "currency":
					typ = "float"
				case "boolean":
					typ = "boolean"
				}
			default:
				log.Debug().Str("name", field.Name).Str("type", field.Type).
					Msg("field with a odd new type")
			}

			fields = append(fields, field)
			fieldNames = append(fieldNames, name+" "+typ)
		}

		log.Print(
			"CREATE TABLE " + sheet.Title + " (" + strings.Join(fieldNames, ",") + ")",
		)
		_, err := db.Exec(
			"CREATE TABLE " + sheet.Title + " (" + strings.Join(fieldNames, ",") + ")",
		)
		if err != nil {
			return err
		}
		log.Debug().Str("table", sheet.Title).Msg("created table")

		log.Debug().Int("count", len(sheet.Records)).Msg("will insert records")
		for _, rec := range sheet.Records {
			values := make([]interface{}, len(fields))
			fplaceholders := make([]string, len(fields))

			for i, field := range fields {
				fplaceholders[i] = "?"

				val, ok := rec[field.Key]
				values[i] = nil
				if ok && val != nil {
					if iv, ok := val.(map[string]interface{}); ok {
						values[i] = iv["value"]
					}
				}
			}

			_, err := db.Exec(
				"INSERT INTO "+sheet.Title+" VALUES ("+strings.Join(fplaceholders, ",")+")",
				values...,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Model struct {
	Sheets      []Sheet   `json:"sheets"`
	Joins       []JoinDef `json:"joins"`
	SideEffects struct {
		Set struct {
			Join map[string]struct {
				Symrefs []JoinEntry `json:"symrefs"`
			} `json:"Join"`
		} `json:"set"`
	} `json:"sideEffects"`
	LocaleSet struct {
		Date   string `json:"date"`
		Number string `json:"number"`
	} `json:"localeSet"`
}

type Sheet struct {
	Id            string   `json:"_id"`
	Title         string   `json:"title"`
	NameFieldMode string   `json:"nameFieldMode"`
	Fields        []Field  `json:"fields"`
	Records       []Record `json:"records"`
}

type Field struct {
	Key         string     `json:"key"`
	Name        string     `json:"name"`
	Type        string     `json:"type"`
	ActualType  string     `json:"-"`
	Enum        []string   `json:"enum"`
	Expression  Expression `json:"expression"`
	LinkedSheet struct {
		Id string `json:"_id"`
	} `json:"linkedSheet"`
}

type Expression struct {
	Op        string       `json:"op"`
	Name      string       `json:"name"`
	Val       string       `json:"val"`
	Key       string       `json:"key"`
	Arguments []Expression `json:"arguments"`
}

type Record map[string]interface{}

type JoinDef struct {
	Id    string   `json:"_id"`
	Right JoinSpec `json:"right"`
	Left  JoinSpec `json:"left"`
}

type JoinSpec struct {
	FieldKey string `json:"fieldKey"`
	SheetId  string `json:"sheetId"`
}

type JoinEntry struct {
	Id     string `json:"_id"`
	JoinId string `json:"joinId"`
	Right  struct {
		Id string `json:"_id"`
	} `json:"right"`
	Left struct {
		Id string `json:"_id"`
	} `json:"left"`
}
