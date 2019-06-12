package misty_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rentziass/misty"
)

func TestObfuscate(t *testing.T) {
	r := strings.NewReader(dump)
	var b bytes.Buffer

	usersTarget := &misty.Target{
		TableName: "public.users",
		Columns: []*misty.TargetColumn{
			{
				Name: "handle",
				Value: func(_ []byte) []byte {
					return []byte("obfuscated_handle")
				},
			},
			{
				Name: "email",
				Value: func(oldVal []byte) []byte {
					if string(oldVal) == "some@example.com" {
						return []byte("obfuscated@email.com")
					}
					return []byte("non_obfuscated@email.com")
				},
			},
		},
	}

	dogsTarget := &misty.Target{
		TableName: "public.dogs",
		Columns: []*misty.TargetColumn{
			{
				Name: "name",
				Value: func(_ []byte) []byte {
					return []byte("doggo")
				},
			},
		},
		DeleteRowRules: []*misty.DeleteRule{
			{
				ColumnName: "name",
				ShouldDelete: func(b []byte) bool {
					return bytes.Equal(b, []byte("Bandit"))
				},
			},
		},
	}
	targets := []*misty.Target{usersTarget, dogsTarget}

	o := misty.NewObfuscator(r, &b, targets)

	err := o.Run()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
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
-- Name: dogs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dogs (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
);

COPY public.dogs (id, name) FROM stdin;
1	Apollo
2	Bailey
3	Bandit
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
1	obfuscated_handle	non_obfuscated@email.com
2	obfuscated_handle	obfuscated@email.com
3	obfuscated_handle	non_obfuscated@email.com
\.

--
-- Name: dogs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dogs (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
);

COPY public.dogs (id, name) FROM stdin;
1	doggo
2	doggo
\.

--
-- PostgreSQL database dump complete
--

`
