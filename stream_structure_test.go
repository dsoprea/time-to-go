package timetogo

import (
	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

// ExampleStreamStructure_Dump shows how to use structure-tracking to print the
// structure of the stream. The `Structure()` and `SetStructureLogging` methods
// (to enable and retrieve the `StreamStructure` struct, if enabled) are
// available on the `StreamBuilder`, ``StreamReader`, `StreamWriter`, and
// `Updater` types.
//
// This table is printed in forward order when writing a stream and reverse
// order when reading a stream.
//
// Columns: 1) absolute offset in the stream, 2) milestone type (describes the
// type of data), 3) scope type (describes whether it's related to streams or
// series, or 'misc' if not enough is known yet during a parse), 4) UUID (only
// for series; usually present for at least all 'series_data_head_byte'
// milestone types), 5) milestone comment (not stored in original data).
func ExampleStreamStructure_Dump() {
	b := rifs.NewSeekableBuffer()
	sb := NewStreamBuilder(b)
	sb.SetStructureLogging(true)

	AddTestSeries(sb)

	_, err := sb.Finish()
	log.PanicIf(err)

	sb.Structure().Dump()

	// Output:
	// ================
	// Stream Structure
	// ================
	//
	// OFF 0        MT series_data_head_byte           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 21       MT series_footer_head_byte         SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 173      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 178      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 179      MT series_data_head_byte           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 206      MT series_footer_head_byte         SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 358      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 363      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 364      MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(2)>
	// OFF 628      MT shadow_footer_head_byte         SCOPE stream   UUID                                           COMM
	// OFF 633      MT boundary_marker                 SCOPE stream   UUID                                           COMM
}
