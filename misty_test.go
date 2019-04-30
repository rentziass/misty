package misty

import (
	"bytes"
	"strings"
	"testing"
)

func TestObfuscate(t *testing.T) {
	r := strings.NewReader(dump)
	var b bytes.Buffer

	target := &Target{
		TableName: "public.users",
		Columns: []*TargetColumn{
			{
				Name: "handle",
				Value: func() []byte {
					return []byte("obfuscated_handle")
				},
			},
			{
				Name: "email",
				Value: func() []byte {
					return []byte("obfuscated@email.com")
				},
			},
		},
	}

	err := Obfuscate(r, &b, []*Target{target})
	if err != nil {
		t.Error("expected no error")
	}

	if b.String() != expectedDump {
		t.Errorf("expected result to be %s, got \n%s instead", expectedDump, b.String())
	}
}

var dump = `--
-- PostgreSQL database dump
--

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    handle character varying(255) NOT NULL,
    email character varying(255)
);

COPY public.users (id, handle, email) FROM stdin;
1	rentziass	\N
2	glannes	some@example.com
3	Onumis	example@some.com
\.

--
-- PostgreSQL database dump complete
--

`

var expectedDump = `--
-- PostgreSQL database dump
--

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    handle character varying(255) NOT NULL,
    email character varying(255)
);

COPY public.users (id, handle, email) FROM stdin;
1	obfuscated_handle	obfuscated@email.com
2	obfuscated_handle	obfuscated@email.com
3	obfuscated_handle	obfuscated@email.com
\.

--
-- PostgreSQL database dump complete
--

`
