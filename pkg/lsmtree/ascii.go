package lsmtree

// ascii table invisible chars

const (
	asciiNull                   = 0x00 // NUL
	asciiStartOfHeading         = 0x01 // SOH
	asciiStartOfText            = 0x02 // STX
	asciiEndOfText              = 0x03 // ETX
	asciiEndOfTransmission      = 0x04 // EOT
	asciiEnquiry                = 0x05 // ENQ
	asciiAcknowledge            = 0x06 // ACK
	asciiBell                   = 0x07 // BEL
	asciiBackspace              = 0x08 // BS
	asciiHorizontalTab          = 0x09 // TAB
	asciiLineFeed               = 0x0A // LF
	asciiVerticalTab            = 0x0B // VT
	asciiFormFeed               = 0x0C // FF
	asciiCarriageReturn         = 0x0D // CR
	asciiShiftOut               = 0x0E // SO
	asciiShiftIn                = 0x0F // SI
	asciiDataLineEscape         = 0x10 // DLE
	asciiDeviceControl1         = 0x11 // DC1
	asciiDeviceControl2         = 0x12 // DC2
	asciiDeviceControl3         = 0x13 // DC3
	asciiDeviceControl4         = 0x14 // DC4
	asciiNegativeAcknowledge    = 0x15 // NAK
	asciiSynchronousIdle        = 0x16 // SYN
	asciiEndOfTransmissionBlock = 0x17 // ETB
	asciiCancel                 = 0x18 // CAN
	asciiEndOfMedium            = 0x19 // EM
	asciiSubstitute             = 0x1A // SUB
	asciiEscape                 = 0x1B // ESC
	asciiFileSeparator          = 0x1C // FS
	asciiGroupSeparator         = 0x1D // GS
	asciiRecordSeparator        = 0x1E // RS
	asciiUnitSeparator          = 0x1F // US
	asciiDelete                 = 0x7F // DEL
)

const (
	NUL = asciiNull
	SOH = asciiStartOfHeading
	STX = asciiStartOfText
	ETX = asciiEndOfText
	EOT = asciiEndOfTransmission
	ENQ = asciiEnquiry
	ACK = asciiAcknowledge
	BEL = asciiBell
	BS  = asciiBackspace
	TAB = asciiHorizontalTab
	LF  = asciiLineFeed // Line feed. Also known as a new line.
	VT  = asciiVerticalTab
	FF  = asciiFormFeed // Form feed. Also known as a new page.
	CR  = asciiCarriageReturn
	SO  = asciiShiftOut
	SI  = asciiShiftIn
	DLE = asciiDataLineEscape
	DC1 = asciiDeviceControl1
	DC2 = asciiDeviceControl2
	DC3 = asciiDeviceControl3
	DC4 = asciiDeviceControl4
	NAK = asciiNegativeAcknowledge
	SYN = asciiSynchronousIdle
	ETB = asciiEndOfTransmissionBlock
	CAN = asciiCancel
	EM  = asciiEndOfMedium
	SUB = asciiSubstitute
	ESC = asciiEscape
	FS  = asciiFileSeparator   // End of file. Or between a concatenation of what might otherwise be separate files.
	GS  = asciiGroupSeparator  // Between sections of data. Not needed in simple data files.
	RS  = asciiRecordSeparator // End of a record or row.
	US  = asciiUnitSeparator   // Between fields of a record, or members of a row.
	DEL = asciiDelete
)
