# [strftime](http://strftime.org/) for Go

[![Build Status](https://travis-ci.org/tebeka/strftime.svg?branch=master)](https://travis-ci.org/tebeka/strftime)

Q: Why? We already have [time.Format](https://golang.org/pkg/time/#Time.Format).

A: Yes, but it becomes tricky to use if if you have string with things other
than time in them. (like `/path/to/%Y/%m/%d/report`)


# Installing

    go get github.com/tebeka/strftime

# Example

    str, err := strftime.Format("%Y/%m/%d", time.Now())

# Contact
https://github.com/tebeka/strftime
    
# License
MIT (see [LICENSE.txt](LICENSE.txt))
