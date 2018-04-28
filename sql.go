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

	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		return err
	}

	model.sheetsById = make(map[string]*Sheet, len(model.Sheets))

	log.Debug().Int("count", len(model.Sheets)).Msg("will insert sheets")
	for _, sheet := range model.Sheets {
		model.sheetsById[sheet.Id] = sheet

		sheet.fieldsByKey = make(map[string]*Field, len(sheet.Fields))
		sheet.tableName = strings.Replace(slug.Make(sheet.Title), "-", "_", -1)

		var fields []*Field
		var fieldNames = []string{"_id text primary key"}

		for _, field := range sheet.Fields {
			sheet.fieldsByKey[field.Key] = field

			name := ""
			typ := "text"

			if field.Name != "" {
				name = strings.Replace(slug.Make(field.Name), "-", "_", -1)
				field.columnName = name
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
				continue
			case "generic":
				log.Debug().Str("name", field.Name).Msg("field")

				// investigating actual type
				for _, rec := range sheet.Records {
					if val, ok := rec[field.Key]; ok {
						if iv, ok := val.(map[string]interface{}); ok {
							if thisType, ok := iv["type"].(string); ok {
								if field.actualType == "" {
									field.actualType = thisType
								} else if field.actualType != thisType {
									// mismatch, let's use 'text'
									log.Debug().Str("act", field.actualType).
										Str("thi", thisType).Msg("mismatch")
								}
							}
						}
					}
				}
				log.Print("actual type: " + field.actualType)
				switch field.actualType {
				case "string", "image", "dayofyear", "file", "email":
					typ = "text"
				case "numeric", "currency", "percent":
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

		_, err := db.Exec(
			"CREATE TABLE " + sheet.tableName +
				" (" + strings.Join(fieldNames, ",") + ")",
		)
		if err != nil {
			return err
		}
		log.Debug().Str("table", sheet.Title).Msg("created table")

		log.Debug().Int("count", len(sheet.Records)).Msg("will insert records")
		for _, rec := range sheet.Records {
			values := make([]interface{}, 1+len(fields))
			fplaceholders := make([]string, 1+len(fields))

			values[0] = rec["_id"]
			fplaceholders[0] = "?"

			for i, field := range fields {
				fplaceholders[i+1] = "?"

				val, ok := rec[field.Key]
				values[i+1] = nil
				if ok && val != nil {
					if iv, ok := val.(map[string]interface{}); ok {
						values[i+1] = iv["value"]
						switch field.actualType {
						case "date":
							spl := strings.Split(iv["value"].(string), "/")
							values[i+1] = spl[2] + "-" + spl[0] + "-" + spl[1]
						}
					}
				}
			}

			_, err := db.Exec(
				"INSERT INTO "+sheet.tableName+" VALUES ("+
					strings.Join(fplaceholders, ",")+")",
				values...,
			)
			if err != nil {
				return err
			}
		}
	}

	// joins must be tracked on separate tables
	model.joinTableNames = make(map[string]string)
	for _, join := range model.Joins {
		sheetLeft := model.sheetsById[join.Left.SheetId]
		sheetRight := model.sheetsById[join.Right.SheetId]

		fieldLeft := sheetLeft.fieldsByKey[join.Left.FieldKey]
		columnLeft := fieldLeft.columnName
		fieldRight := sheetRight.fieldsByKey[join.Right.FieldKey]
		columnRight := fieldRight.columnName
		if fieldLeft.columnName == "" {
			columnLeft = sheetRight.tableName
		}
		if fieldRight.columnName == "" {
			columnRight = sheetLeft.tableName
		}

		joinTableName := "join=" + sheetLeft.tableName + ":" + columnLeft + "/" + sheetRight.tableName + ":" + columnRight
		model.joinTableNames[join.Id] = joinTableName

		_, err := db.Exec(`
CREATE TABLE '` + joinTableName + `' (
  left text,
  right text,
  FOREIGN KEY (left) REFERENCES ` + sheetLeft.tableName + `(_id)
  FOREIGN KEY (right) REFERENCES ` + sheetRight.tableName + `(_id)
)
            `)
		if err != nil {
			return err
		}

		for _, ref := range model.SideEffects.Set.Join[join.Id].Symrefs {
			_, err := db.Exec(
				"INSERT INTO '"+model.joinTableNames[ref.JoinId]+"' "+
					"VALUES (?, ?)",
				ref.Left.Id, ref.Right.Id,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type Model struct {
	Sheets         []*Sheet `json:"sheets"`
	sheetsById     map[string]*Sheet
	Joins          []JoinDef `json:"joins"`
	joinTableNames map[string]string
	SideEffects    struct {
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
	Id            string `json:"_id"`
	Title         string `json:"title"`
	tableName     string
	NameFieldMode string `json:"nameFieldMode"`
	fieldsByKey   map[string]*Field
	Fields        []*Field `json:"fields"`
	Records       []Record `json:"records"`
}

type Field struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	columnName  string
	Type        string `json:"type"`
	actualType  string
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
