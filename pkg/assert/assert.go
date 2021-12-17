package assert

import "log"

func AssertTrue(isTrue bool) {
	if isTrue == true {
		log.Printf("assert true: condition=%t", isTrue)
	}
}

func AssertFalse(isFalse bool) {
	if isFalse == false {
		log.Printf("assert false: condition=%t", isFalse)
	}
}

func AssertTrueMsg(isTrue bool, msg string) {
	if isTrue == true {
		log.Printf("%s, condition=%t", msg, isTrue)
	}
}

func AssertFalseMsg(isFalse bool, msg string) {
	if isFalse == false {
		log.Printf("%s, condition=%t", msg, isFalse)
	}
}

func AssertIfErr(err error) {
	AssertIfErrMsg(err, "error")
}

func AssertIfErrMsg(err error, msg string) {
	if err != nil {
		log.Printf("%s %+v", msg, err)
	}
}

func InfoIfTrue(isTrue bool, msg string) {
	if isTrue == true {
		log.Printf("%s, condition=%t", msg, isTrue)
	}
}

func InfoIfFalse(isFalse bool, msg string) {
	if isFalse == false {
		log.Printf("%s, condition=%t", msg, isFalse)
	}
}

func InfoIfErr(err error) {
	InfoIfErrMsg(err, "error")
}

func InfoIfErrMsg(err error, msg string) {
	if err != nil {
		log.Printf("%s %+v", msg, err)
	}
}

func WarnIfTrue(isTrue bool, msg string) {
	if isTrue == true {
		log.Printf("%s, condition=%t", msg, isTrue)
	}
}

func WarnIfFalse(isFalse bool, msg string) {
	if isFalse == false {
		log.Printf("%s, condition=%t", msg, isFalse)
	}
}

func WarnIfErr(err error) {
	WarnIfErrMsg(err, "error")
}

func WarnIfErrMsg(err error, msg string) {
	if err != nil {
		log.Printf("%s %+v", msg, err)
	}
}

func PanicIfTrue(isTrue bool, msg string) {
	if isTrue == true {
		log.Panicf("%s, condition=%t", msg, isTrue)
	}
}

func PanicIfFalse(isFalse bool, msg string) {
	if isFalse == false {
		log.Panicf("%s, condition=%t", msg, isFalse)
	}
}

func PanicIfErr(err error) {
	PanicIfErrMsg(err, "error")
}

func PanicIfErrMsg(err error, msg string) {
	if err != nil {
		log.Panicf("%s %+v", msg, err)
	}
}

func FailIfTrue(isTrue bool, msg string) {
	if isTrue == true {
		log.Fatalf("%s, condition=%t", msg, isTrue)
	}
}

func FailIfFalse(isFalse bool, msg string) {
	if isFalse == false {
		log.Fatalf("%s, condition=%t", msg, isFalse)
	}
}

func FailIfErr(err error) {
	if err != nil {
		log.Fatalf("error %+v", err)
	}
}

func FailIfErrMsg(err error, msg string) {
	if err != nil {
		log.Fatalf("%s %+v", msg, err)
	}
}
