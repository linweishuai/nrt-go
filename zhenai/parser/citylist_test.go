package parser

import (
	"io/ioutil"
	"testing"
)

func TestParseCityList(t *testing.T) {
	contents, _ := ioutil.ReadFile("citylist_test_data.html")
	result := ParseCityList(contents)

	const resultSize = 408
	expectedUrls := []string{
		"http://www.zhenai.com/zhenghun/aba",
		"http://www.zhenai.com/zhenghun/zunyi",
	}
	expectedItems := []string{
		"城市: 阿坝",
		"城市: 遵义",
	}
	if len(result.Items) != resultSize {
		t.Errorf("items is not 408,is %v", len(result.Items))
	}
	if len(result.Requests) != resultSize {
		t.Errorf("requests is not 408,is %v", len(result.Requests))
	}

	if result.Requests[0].Url != expectedUrls[0] {
		t.Errorf("begin url is not aba,is %s", result.Requests[0].Url)
	}
	if result.Items[0].(string) != expectedItems[0] {
		t.Errorf("begin item is not 阿坝,is %s", result.Items[0].(string))
	}

	if result.Requests[len(result.Requests)-1].Url != expectedUrls[1] {
		t.Errorf("end url is not zunyi,is %s", result.Requests[len(result.Requests)-1].Url)
	}
	if result.Items[len(result.Items)-1].(string) != expectedItems[1] {
		t.Errorf("end item is not 遵义,is %s", result.Items[len(result.Items)-1].(string))
	}
}
