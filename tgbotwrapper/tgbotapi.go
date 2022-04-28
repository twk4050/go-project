package tgbotwrapper

import tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

func SendMessage(token string, chat_id int64, textMessage string, markdown bool) {

	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		panic(err)
	}

	msg := tgbotapi.NewMessage(chat_id, "")
	if markdown {
		msg.ParseMode = tgbotapi.ModeMarkdownV2 // "MarkdownV2"
		msg.Text = formatToMarkdownCodeBlock(textMessage)
		bot.Send(msg)
		return
	}

	msg.Text = textMessage
	bot.Send(msg)
}

func formatToMarkdownCodeBlock(s string) string {
	return "```" + s + "```"
}