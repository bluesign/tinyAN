package ui

import (
	"github.com/bluesign/tinyAN/storage"
	"github.com/dustin/go-humanize"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func MainUI(store *storage.HeightBasedStorage) {
	app := tview.NewApplication()

	//sporks := tview.NewBox().SetBorder(true).SetTitle("Sporks")

	sporksData := tview.NewTable().
		SetBorders(false)

	sporksData.Box.SetTitle("Sporks")
	sporksData.Box.SetBorder(true)
	sporksData.Box.SetBorderPadding(0, 0, 1, 1)

	for i, spork := range store.Sporks() {

		sporksData.SetCell(i*7, 0, tview.NewTableCell(spork.Name()).SetAlign(tview.AlignLeft).SetTextColor(tcell.ColorYellow))

		for j, header := range []string{"Start Height", "End Height", "Blocks Index", "Ledger Index", "EVM Index"} {
			sporksData.SetCell(j+1+i*7, 0,
				tview.NewTableCell("  "+header+"  ").
					SetTextColor(tcell.ColorLightGray).
					SetAlign(tview.AlignLeft))

			value := ""
			switch j {
			case 0:
				value = humanize.Comma(int64(spork.StartHeight()))
			case 1:

				value = humanize.Comma(int64(spork.EndHeight()))
				if spork.EndHeight() == 0 {
					value = "Live"
				}
			case 2:
				value = humanize.Comma(int64(spork.Protocol().LastProcessedHeight()))
			case 3:
				value = humanize.Comma(int64(spork.Ledger().LastProcessedHeight()))
			case 4:
				value = humanize.Comma(int64(spork.EVM().LastProcessedHeight()))
			}

			sporksData.SetCell(j+1+i*7, 1,
				tview.NewTableCell(value).
					SetTextColor(tcell.ColorWhite).
					SetAlign(tview.AlignRight))
		}
	}

	flex := tview.NewFlex().
		AddItem(sporksData, 40, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(tview.NewBox().SetBorder(true).SetTitle("Middle (3 x height of Top)"), 0, 3, false).
			AddItem(tview.NewBox().SetBorder(true).SetTitle("Live Log Stream"), 0, 1, false), 0, 2, false).
		AddItem(tview.NewBox().SetBorder(true).SetTitle("Services"), 40, 1, false)

	if err := app.SetRoot(flex, true).SetFocus(flex).Run(); err != nil {
		panic(err)
	}

}
