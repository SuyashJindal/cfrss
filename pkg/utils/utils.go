package utils

import (
	"strings"

	"github.com/google/uuid"

	"github.com/variety-jones/cfrss/pkg/models"
)

func GetNewUUID() string {
	return uuid.New().String()
}

func ConvertRelativeLinksToAbsoluteLinks(actions []models.RecentAction) {
	for ind := range actions {
		if actions[ind].Comment == nil {
			continue
		}
		actions[ind].Comment.Text = strings.ReplaceAll(actions[ind].Comment.Text,
			"href=\"/", "href=\"https://codeforces.com/")
	}
}
