package main

import (
	"io"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type Settings struct {
	Port string `envconfig:"PORT" default:"5000"`
}

var err error
var log = zerolog.New(os.Stderr).Output(zerolog.ConsoleWriter{Out: os.Stderr})
var s Settings
var router *mux.Router

func main() {
	log.Debug().Msg("starting app...")

	err = envconfig.Process("", &s)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed when loading environment variables.")
	}
	log.Debug().Msg("...settings loaded.")

	// routes
	router = mux.NewRouter()

	router.HandleFunc("/book/{book}", func(w http.ResponseWriter, r *http.Request) {
		book := mux.Vars(r)["book"]
		log.Info().Str("book", book).Msg("book requested")

		if _, err := os.Stat(book + ".db"); err == nil {
			log.Print("db cached matched")
			goto serve
		}

		if _, err := os.Stat(book + ".json"); os.IsNotExist(err) {
			log.Print("downloading file from fieldbook")
			out, _ := os.Create(book + ".json")
			defer out.Close()
			resp, err := http.Get("https://fieldbook.com/books/" + book + ".json")
			if err != nil {
				log.Warn().Err(err).Str("book", book).
					Msg("failed to find fieldbook")
				http.Error(w, "couldn't find your book, is the id correct?", 403)
				return
			}
			defer resp.Body.Close()
			_, err = io.Copy(out, resp.Body)
			if err != nil {
				log.Warn().Err(err).Str("book", book).
					Msg("failed to download fieldbook")
				http.Error(w, "failed to fetch your book. please report this issue.", 503)
				return
			}
		}

		log.Print("building sqlite database")
		err = buildsqlite(book+".json", book+".db")
		if err != nil {
			log.Warn().Err(err).Str("book", book).
				Msg("failed to build sqlite database")
			http.Error(w, "failed to build database. please report this issue.", 500)
			return
		}

	serve:
		w.Header().Set("Access-Control-Allow-Origin", "*")
		http.ServeFile(w, r, book+".db")
		return
	})

	log.Debug().Msg("...routes declared.")

	log.Info().Str("port", s.Port).Msg("started listening.")
	http.ListenAndServe(":"+s.Port, router)
	log.Info().Msg("exiting...")
}
