package tgbotwrapper

import (
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func SendMessage(token string, chat_id int64, textMessage string) {
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		panic(err)
	}

	bot.Send(tgbotapi.NewMessage(chat_id, textMessage))
}