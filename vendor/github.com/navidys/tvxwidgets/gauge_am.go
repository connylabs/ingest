package tvxwidgets

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

//ActivityModeGauge represents activity mode gauge permitive.
type ActivityModeGauge struct {
	*tview.Box
	// counter value
	counter int

	// pgBgColor: progress block background color
	pgBgColor tcell.Color
}

// NewActivityModeGauge returns new activity mode gauge permitive.
func NewActivityModeGauge() *ActivityModeGauge {
	gauge := &ActivityModeGauge{
		Box:       tview.NewBox(),
		counter:   0,
		pgBgColor: tcell.ColorBlue,
	}
	return gauge
}

// Draw draws this primitive onto the screen.
func (g *ActivityModeGauge) Draw(screen tcell.Screen) {
	g.Box.DrawForSubclass(screen, g)
	x, y, width, height := g.Box.GetInnerRect()
	tickStr := g.tickStr(width)
	for i := 0; i < height; i++ {
		tview.Print(screen, tickStr, x, y+i, width, tview.AlignLeft, g.pgBgColor)
	}
}

// SetTitle sets title for this primitive.
func (g *ActivityModeGauge) SetTitle(title string) {
	g.Box.SetTitle(title)
}

// Focus is called when this primitive receives focus
func (g *ActivityModeGauge) Focus(delegate func(p tview.Primitive)) {
}

// HasFocus returns whether or not this primitive has focus
func (g *ActivityModeGauge) HasFocus() bool {
	return g.Box.HasFocus()
}

// GetRect return primitive current rect
func (g *ActivityModeGauge) GetRect() (int, int, int, int) {
	return g.Box.GetRect()
}

// SetRect sets rect for this primitive.
func (g *ActivityModeGauge) SetRect(x, y, width, height int) {
	g.Box.SetRect(x, y, width, height)
}

// SetPgBgColor sets progress block background color
func (g *ActivityModeGauge) SetPgBgColor(color tcell.Color) {
	g.pgBgColor = color
}

// Pulse pulse update the gauge progress bar
func (g *ActivityModeGauge) Pulse() {
	g.counter = g.counter + 1
}

// Reset resets the gauge counter (set to 0)
func (g *ActivityModeGauge) Reset() {
	g.counter = 0
}

func (g *ActivityModeGauge) tickStr(max int) string {
	if g.counter >= max-4 {
		g.counter = 0
	}
	prgHeadStr := ""
	hWidth := 0
	prgEndStr := ""
	prgStr := ""
	for i := 0; i < g.counter; i++ {
		prgHeadStr = prgHeadStr + fmt.Sprintf("[%s::]%s", getColorName(tview.Styles.PrimitiveBackgroundColor), prgCell)
		hWidth++
	}
	prgStr = prgCell + prgCell + prgCell + prgCell
	for i := 0; i < max+hWidth+4; i++ {
		prgEndStr = prgEndStr + fmt.Sprintf("[%s::]%s", getColorName(tview.Styles.PrimitiveBackgroundColor), prgCell)
	}

	return fmt.Sprintf("%s[%s::]%s%s", prgHeadStr, getColorName(g.pgBgColor), prgStr, prgEndStr)
}
