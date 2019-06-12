package misty_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rentziass/misty"
)

func TestBuildTablesIndex(t *testing.T) {
	r := strings.NewReader(dump)
	index := misty.BuildTablesIndex(r)

	assert.Len(t, index, 2)

	users := index[0]
	assert.Equal(t, "public.users", users.Name)
	assert.Equal(t, 15, users.StartingLine)
	assert.EqualValues(t, 237, users.StartingAt)
	assert.EqualValues(t, 357, users.EndingAt)

	dogs := index[1]
	assert.Equal(t, "public.dogs", dogs.Name)
	assert.Equal(t, 30, dogs.StartingLine)
	assert.EqualValues(t, 524, dogs.StartingAt)
	assert.EqualValues(t, 593, dogs.EndingAt)
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
