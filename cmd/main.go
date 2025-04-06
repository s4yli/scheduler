package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/zhashkevych/scheduler"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"
)

const (
	configAPI = "http://localhost:8080/resources"
	natsURL   = "nats://localhost:4222"
	stream    = "TIMETABLE"
	subject   = "TIMETABLE.EVENTS"
)

var jsc nats.JetStreamContext

func main() {
	// Initialiser JetStream
	err := initStream()
	if err != nil {
		log.Fatal("Erreur d'initialisation du stream:", err)
	}

	// Initialiser le scheduler
	ctx := context.Background()
	sc := scheduler.NewScheduler()

	// Ajouter la tâche récurrente
	sc.Add(ctx, fetchAndPublishEvents, 30*time.Second)
	fmt.Println("Tâche programmée ajoutée, elle s'exécutera toutes les 30 secondes.")

	// Garder le programme en vie jusqu'à une interruption
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Signal d'interruption reçu, arrêt du scheduler.")
	sc.Stop()
}

// initStream initialise un stream dans JetStream s'il n'existe pas déjà
func initStream() error {
	fmt.Println("Initialisation de JetStream...")
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}
	jsc, err = nc.JetStream()
	if err != nil {
		return err
	}

	// Vérifier si le stream existe déjà
	streamInfo, err := jsc.StreamInfo(stream)
	if err == nil && streamInfo != nil {
		fmt.Println("Le stream existe déjà:", stream)
		return nil
	}

	// Créer un stream s'il n'existe pas
	_, err = jsc.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject}, // Associer le stream à un sujet
	})
	if err != nil {
		return err
	}
	fmt.Println("Stream créé avec succès:", stream)
	return nil
}

// fetchResources récupère les ressources depuis Config
func fetchResources() ([]Resource, error) {
	fmt.Println("Récupération des ressources depuis l'API Config...")
	resp, err := http.Get(configAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var resources []Resource
	err = json.NewDecoder(resp.Body).Decode(&resources)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Ressources récupérées: %+v\n", resources)
	return resources, nil
}

// buildEdtURL génère l'URL pour récupérer les événements pour une ressource donnée
func buildEdtURL(resourceUcaID int) string {
	url := fmt.Sprintf("https://edt.uca.fr/jsp/custom/modules/plannings/anonymous_cal.jsp?resources=%d&projectId=2&calType=ical&nbWeeks=8&displayConfigId=128", resourceUcaID)
	fmt.Printf("URL générée pour la ressource %d: %s\n", resourceUcaID, url)
	return url
}

// fetchICalendar récupère les événements bruts depuis l'URL EDT et les sauvegarde dans un fichier .ics
func fetchICalendar(url string) (string, error) {
	fmt.Println("Récupération du fichier iCalendar depuis l'URL:", url)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Sauvegarder le contenu dans un fichier .ics
	file, err := os.Create("events.ics")
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}

	return "events.ics", nil
}

// parseICalendarFromFile extrait les événements depuis un fichier .ics
func parseICalendarFromFile(filePath string) ([]Event, error) {
	fmt.Println("Analyse du fichier iCalendar:", filePath)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	eventRegex := regexp.MustCompile(`(?s)BEGIN:VEVENT(.*?)END:VEVENT`)
	matches := eventRegex.FindAllStringSubmatch(string(data), -1)

	eventMap := make(map[string]Event)

	for _, match := range matches {
		eventData := match[1]
		event := Event{}
		lines := strings.Split(eventData, "\r\n")

		var currentField string
		var currentValue strings.Builder

		for _, line := range lines {
			if line == "" {
				continue
			}

			// Gestion des lignes pliées (commençant par un espace)
			if strings.HasPrefix(line, " ") {
				if currentField != "" {
					currentValue.WriteString(strings.TrimPrefix(line, " "))
				}
				continue
			}

			// Traitement du champ précédent
			if currentField != "" {
				value := strings.TrimSpace(currentValue.String())
				switch currentField {
				case "UID":
					event.Id = value
				case "DTSTAMP":
					event.Dtstamp = value
				case "DTSTART":
					event.Dtstart = value
				case "DTEND":
					event.Dtend = value
				case "DESCRIPTION":
					event.Description = value
				case "LOCATION":
					event.Location = value
				case "CREATED":
					event.Created = value
				case "LAST-MODIFIED":
					event.LastModified = value
				}
			}

			// Nouveau champ
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}

			currentField = parts[0]
			currentValue.Reset()
			currentValue.WriteString(parts[1])
		}

		// Dernier champ
		if currentField != "" {
			value := strings.TrimSpace(currentValue.String())
			switch currentField {
			case "UID":
				event.Id = value
			case "DTSTAMP":
				event.Dtstamp = value
			case "DTSTART":
				event.Dtstart = value
			case "DTEND":
				event.Dtend = value
			case "DESCRIPTION":
				event.Description = value
			case "LOCATION":
				event.Location = value
			case "CREATED":
				event.Created = value
			case "LAST-MODIFIED":
				event.LastModified = value
			}
		}

		if event.Id != "" {
			eventMap[event.Id] = event
		}
	}

	events := make([]Event, 0, len(eventMap))
	for _, event := range eventMap {
		events = append(events, event)
	}

	fmt.Printf("Nombre d'événements extraits : %d\n", len(events))
	return events, nil
}

// publishEvents envoie les événements via JetStream
func publishEvents(events []Event) error {
	fmt.Println("Publication des événements dans JetStream...")
	for _, event := range events {
		fmt.Printf("📌 Avant la sérialisation : %+v\n", event)
		eventJSON, err := json.Marshal(event)
		if err != nil {
			fmt.Println("Erreur de sérialisation JSON:", err)
			continue
		}

		fmt.Printf("Envoi de l'événement : %s\n", string(eventJSON))
		pubAckFuture, err := jsc.PublishAsync(subject, eventJSON)
		if err != nil {
			fmt.Println("Erreur d'envoi à JetStream:", err)
			continue
		}

		select {
		case ack := <-pubAckFuture.Ok():
			fmt.Println("Événement publié avec succès:", ack.Stream, ack.Sequence)
		case err := <-pubAckFuture.Err():
			fmt.Println("Erreur d'ACK JetStream:", err)
		}
	}
	return nil
}

// fetchAndPublishEvents récupère et publie les événements pour chaque ressource
func fetchAndPublishEvents(ctx context.Context) {
	fmt.Println("Exécution de la tâche programmée...")

	resources, err := fetchResources()
	if err != nil {
		fmt.Println("Erreur lors de la récupération des ressources:", err)
		return
	}

	// Pour chaque ressource, récupérer et publier les événements individuellement
	for _, resource := range resources {
		edtURL := buildEdtURL(resource.UcaId)
		filePath, err := fetchICalendar(edtURL)
		if err != nil {
			fmt.Printf("Erreur lors de la récupération du fichier iCalendar pour la ressource %d: %v\n", resource.UcaId, err)
			continue
		}

		// Analyse du fichier ICS
		events, err := parseICalendarFromFile(filePath)
		if err != nil {
			fmt.Printf("Erreur lors de l'analyse du fichier iCalendar pour la ressource %d: %v\n", resource.UcaId, err)
			continue
		}

		// Ajouter l'id de la ressource à chaque événement
		for i := range events {
			events[i].ResourceID = resource.Id
		}

		// Publication des événements pour cette ressource
		err = publishEvents(events)
		if err != nil {
			fmt.Printf("Erreur lors de l'envoi des événements à JetStream pour la ressource %d: %v\n", resource.UcaId, err)
		}
	}
}

// Structures pour les ressources et événements

// Nouvelle structure Resource incluant l'id (UUID sous forme de chaîne)
type Resource struct {
	Id    string `json:"id"`
	UcaId int    `json:"uca_id"`
	Name  string `json:"name"`
}

// Structure Event modifiée pour inclure le champ ResourceID
type Event struct {
	Id           string `json:"UID"`
	Dtstamp      string `json:"DTSTAMP"`
	Dtstart      string `json:"DTSTART"`
	Dtend        string `json:"DTEND"`
	Description  string `json:"DESCRIPTION"`
	Location     string `json:"LOCATION"`
	Created      string `json:"CREATED"`
	LastModified string `json:"LAST-MODIFIED"`
	ResourceID   string `json:"RESOURCE-ID"`
}
