package services

import (
	"testing"
)

func TestNormalizePunctuation(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Lata Mangeshkar， Kishore Kumar", "Lata Mangeshkar, Kishore Kumar"},
		{"Artist1／Artist2", "Artist1 / Artist2"},
		{"Composer1； Composer2", "Composer1; Composer2"},
		{"Standard, Normal - Title", "Standard, Normal - Title"},
		{"", ""},
	}

	for _, tt := range tests {
		actual := NormalizePunctuation(tt.input)
		if actual != tt.expected {
			t.Errorf("NormalizePunctuation(%q) = %q; expected %q", tt.input, actual, tt.expected)
		}
	}
}

func TestCleanDuplicatedPhrase(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		shouldMatch bool
	}{
		{"Chand Sifarish, Chand Sifarish", "Chand Sifarish", true},
		{"TUMSE MILNA, TUMSE MILNA", "TUMSE MILNA", true},
		{"JHALAK DIKHLA JA, JHALAK DIKHLA JA", "JHALAK DIKHLA JA", true},
		{"Shaan, Kailash Kher, Shaan, Kailash Kher", "Shaan, Kailash Kher", true},
		{"Artist / Band / Artist / Band", "Artist / Band", true},
		{"Part1; Part2; Part1; Part2", "Part1; Part2", true},
		{"Song - Song", "Song", true},
		{"No Duplication", "No Duplication", false},
		{"Girls, Girls, Girls", "Girls, Girls, Girls", false}, // 3 parts, not halved
		{"No, No, No", "No, No, No", false},
		{"Short", "Short", false},
	}

	for _, tt := range tests {
		actual, matched := CleanDuplicatedPhrase(tt.input)
		if matched != tt.shouldMatch {
			t.Errorf("CleanDuplicatedPhrase(%q) matched = %v; expected %v", tt.input, matched, tt.shouldMatch)
		}
		if matched && actual != tt.expected {
			t.Errorf("CleanDuplicatedPhrase(%q) = %q; expected %q", tt.input, actual, tt.expected)
		}
	}
}

func TestCleanMetadata_RealCorruptedExamples(t *testing.T) {
	cases := []struct {
		name          string
		rawTitle      string
		rawArtist     string
		expectedTitle string
		expectedArt   string
	}{
		{
			name:          "Chand Sifarish",
			rawTitle:      "Chand Sifarish, Chand Sifarish",
			rawArtist:     "Shaan, Kailash Kher, Shaan, Kailash Kher",
			expectedTitle: "Chand Sifarish",
			expectedArt:   "Shaan, Kailash Kher",
		},
		{
			name:          "Tumse Milna",
			rawTitle:      "TUMSE MILNA, TUMSE MILNA",
			rawArtist:     "UDIT NARAYAN, ALKA YAGNIK, HIMESH RESHAMMIYA, SAMEER, UDIT NARAYAN, ALKA YAGNIK, HIMESH RESHAMMIYA, SAMEER",
			expectedTitle: "TUMSE MILNA",
			expectedArt:   "UDIT NARAYAN, ALKA YAGNIK, HIMESH RESHAMMIYA, SAMEER",
		},
		{
			name:          "Jhalak Dikhla Ja",
			rawTitle:      "JHALAK DIKHLA JA, JHALAK DIKHLA JA",
			rawArtist:     "HIMESH RESHAMMIYA, SAMEER, HIMESH RESHAMMIYA, SAMEER",
			expectedTitle: "JHALAK DIKHLA JA",
			expectedArt:   "HIMESH RESHAMMIYA, SAMEER",
		},
		{
			name:          "Tera Surroor",
			rawTitle:      "Tera Surroor, Tera Surroor",
			rawArtist:     "Himesh Reshammiya, Himesh Reshammiya",
			expectedTitle: "Tera Surroor",
			expectedArt:   "Himesh Reshammiya",
		},
		{
			name:          "Naam Hai Tera",
			rawTitle:      "Naam Hai Tera, Naam Hai Tera",
			rawArtist:     "Himesh Reshammiya, Himesh Reshammiya",
			expectedTitle: "Naam Hai Tera",
			expectedArt:   "Himesh Reshammiya",
		},
		{
			name:          "Tera Chehra",
			rawTitle:      "Tera Chehra, Tera Chehra",
			rawArtist:     "Adnan Sami, Adnan Sami",
			expectedTitle: "Tera Chehra",
			expectedArt:   "Adnan Sami",
		},
		{
			name:          "Obujh Bhalobasha",
			rawTitle:      "Obujh Bhalobasha, Obujh Bhalobasha",
			rawArtist:     "Hridoy Khan, Saara, Maliha Tajnin Tani, Hridoy Khan, Saara, Maliha Tajnin Tani",
			expectedTitle: "Obujh Bhalobasha",
			expectedArt:   "Hridoy Khan, Saara, Maliha Tajnin Tani",
		},
		{
			name:          "Ab Ke Sawan with Full-width comma",
			rawTitle:      "Ab Ke Sawan Mein Jee Dare, Ab Ke Sawan Mein Jee Dare",
			rawArtist:     "Lata Mangeshkar， Kishore Kumar, Lata Mangeshkar， Kishore Kumar",
			expectedTitle: "Ab Ke Sawan Mein Jee Dare",
			expectedArt:   "Lata Mangeshkar, Kishore Kumar",
		},
		{
			name:          "O Sanam",
			rawTitle:      "O Sanam, O Sanam",
			rawArtist:     "Lucky Ali, Lucky Ali",
			expectedTitle: "O Sanam",
			expectedArt:   "Lucky Ali",
		},
		{
			name:          "Akhiyaan",
			rawTitle:      "Akhiyaan, Akhiyaan",
			rawArtist:     "Mitraz, Mitraz",
			expectedTitle: "Akhiyaan",
			expectedArt:   "Mitraz",
		},
		{
			name:          "Mangal Bhavan",
			rawTitle:      "Mangal Bhavan Amangal Haari Sitaram Charit, Mangal Bhavan Amangal Haari Sitaram Charit",
			rawArtist:     "Ravindra Jain, Anand Kumar C., Ravindra Jain, Anand Kumar C.",
			expectedTitle: "Mangal Bhavan Amangal Haari Sitaram Charit",
			expectedArt:   "Ravindra Jain, Anand Kumar C.",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanTitle, cleanArtist := CleanMetadata(tc.rawTitle, tc.rawArtist)
			if cleanTitle != tc.expectedTitle {
				t.Errorf("CleanMetadata title = %q; expected %q", cleanTitle, tc.expectedTitle)
			}
			if cleanArtist != tc.expectedArt {
				t.Errorf("CleanMetadata artist = %q; expected %q", cleanArtist, tc.expectedArt)
			}
		})
	}
}

func TestCleanMetadata_FalsePositiveGuardrails(t *testing.T) {
	cases := []struct {
		name          string
		rawTitle      string
		rawArtist     string
		expectedTitle string
		expectedArt   string
	}{
		{
			name:          "Sei Bhalo, Sei Bhalo (Rabindrasangeet - preserve)",
			rawTitle:      "Sei Bhalo, Sei Bhalo",
			rawArtist:     "Shaan",
			expectedTitle: "Sei Bhalo, Sei Bhalo",
			expectedArt:   "Shaan",
		},
		{
			name:          "Tonight, Tonight (Smashing Pumpkins - preserve)",
			rawTitle:      "Tonight, Tonight",
			rawArtist:     "The Smashing Pumpkins",
			expectedTitle: "Tonight, Tonight",
			expectedArt:   "The Smashing Pumpkins",
		},
		{
			name:          "Girls, Girls, Girls (Motley Crue - 3 parts - preserve)",
			rawTitle:      "Girls, Girls, Girls",
			rawArtist:     "Mötley Crüe",
			expectedTitle: "Girls, Girls, Girls",
			expectedArt:   "Mötley Crüe",
		},
		{
			name:          "No, No, No (Destiny's Child - 3 parts - preserve)",
			rawTitle:      "No, No, No",
			rawArtist:     "Destiny's Child",
			expectedTitle: "No, No, No",
			expectedArt:   "Destiny's Child",
		},
		{
			name:          "Long repeated title without artist duplication (clean)",
			rawTitle:      "Mangal Bhavan Amangal Haari Sitaram Charit, Mangal Bhavan Amangal Haari Sitaram Charit",
			rawArtist:     "Ravindra Jain",
			expectedTitle: "Mangal Bhavan Amangal Haari Sitaram Charit",
			expectedArt:   "Ravindra Jain",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanTitle, cleanArtist := CleanMetadata(tc.rawTitle, tc.rawArtist)
			if cleanTitle != tc.expectedTitle {
				t.Errorf("CleanMetadata title = %q; expected %q", cleanTitle, tc.expectedTitle)
			}
			if cleanArtist != tc.expectedArt {
				t.Errorf("CleanMetadata artist = %q; expected %q", cleanArtist, tc.expectedArt)
			}
		})
	}
}
