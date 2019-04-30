package misty

import (
	"bytes"
	"strings"
	"testing"
)

func TestObfuscate(t *testing.T) {
	r := strings.NewReader(dump)
	var b bytes.Buffer

	usersTarget := &Target{
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

	dogsTarget := &Target{
		TableName: "public.dogs",
		Columns: []*TargetColumn{
			{
				Name: "name",
				Value: func() []byte {
					return []byte("doggo")
				},
			},
		},
	}

	err := Obfuscate(r, &b, []*Target{usersTarget, dogsTarget})
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
1	obfuscated_handle	obfuscated@email.com
2	obfuscated_handle	obfuscated@email.com
3	obfuscated_handle	obfuscated@email.com
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
3	doggo
\.

--
-- PostgreSQL database dump complete
--

`
