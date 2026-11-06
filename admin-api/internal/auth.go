package internal

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	jwksTTL     = time.Hour
	jwksTimeout = 10 * time.Second
)

type cognitoClaimsKey struct{}

type jwks struct {
	Keys []jwkKey `json:"keys"`
}

type jwkKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type CognitoValidator struct {
	issuer     string
	jwksURL    string
	httpClient *http.Client
	mu         sync.RWMutex
	keys       map[string]*rsa.PublicKey
	fetchedAt  time.Time
}

func NewCognitoValidator(region, userPoolID string) *CognitoValidator {
	issuer := fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/%s", region, userPoolID)
	return &CognitoValidator{
		issuer:     issuer,
		jwksURL:    issuer + "/.well-known/jwks.json",
		httpClient: &http.Client{Timeout: jwksTimeout},
		keys:       make(map[string]*rsa.PublicKey),
	}
}

func (v *CognitoValidator) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}
			token, err := v.validateRequest(r)
			if err != nil {
				writeUnauthorized(w, err.Error())
				return
			}
			ctx := context.WithValue(r.Context(), cognitoClaimsKey{}, token.Claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func (v *CognitoValidator) validateRequest(r *http.Request) (*jwt.Token, error) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, errors.New("missing or invalid Authorization header")
	}
	raw := strings.TrimPrefix(header, "Bearer ")

	token, err := jwt.Parse(raw,
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			kid, ok := t.Header["kid"].(string)
			if !ok {
				return nil, errors.New("missing kid in token header")
			}
			return v.getKey(kid)
		},
		jwt.WithIssuer(v.issuer),
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims format")
	}
	if tokenUse, _ := claims["token_use"].(string); tokenUse != "access" {
		return nil, errors.New("token_use must be access")
	}

	return token, nil
}

func (v *CognitoValidator) getKey(kid string) (*rsa.PublicKey, error) {
	v.mu.RLock()
	key, ok := v.keys[kid]
	stale := time.Since(v.fetchedAt) > jwksTTL
	v.mu.RUnlock()

	if ok && !stale {
		return key, nil
	}

	if err := v.refreshKeys(); err != nil {
		return nil, fmt.Errorf("failed to refresh JWKS: %w", err)
	}

	v.mu.RLock()
	key, ok = v.keys[kid]
	v.mu.RUnlock()

	if !ok {
		return nil, errors.New("unknown kid — key not found in Cognito JWKS")
	}
	return key, nil
}

func (v *CognitoValidator) refreshKeys() error {
	resp, err := v.httpClient.Get(v.jwksURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var j jwks
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		return err
	}

	keys := make(map[string]*rsa.PublicKey, len(j.Keys))
	for _, k := range j.Keys {
		if k.Kty != "RSA" || k.Use != "sig" {
			continue
		}
		pub, err := parseRSAPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		keys[k.Kid] = pub
	}

	v.mu.Lock()
	v.keys = keys
	v.fetchedAt = time.Now()
	v.mu.Unlock()
	return nil
}

func parseRSAPublicKey(n, e string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(n)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(e)
	if err != nil {
		return nil, err
	}
	nInt := new(big.Int).SetBytes(nBytes)
	var eInt int
	for _, b := range eBytes {
		eInt = eInt<<8 + int(b)
	}
	return &rsa.PublicKey{N: nInt, E: eInt}, nil
}

func writeUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}
