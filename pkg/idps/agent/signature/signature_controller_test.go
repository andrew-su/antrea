// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package signature

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	fakerest "k8s.io/client-go/rest/fake"

	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	fakeversioned "antrea.io/antrea/pkg/client/clientset/versioned/fake"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
	suricatatest "antrea.io/antrea/pkg/idps/agent/suricata/testing"
	bytesutil "antrea.io/antrea/pkg/idps/util/bytes"
)

type idpsClientGetter struct {
	clientset rest.Interface
}

func (g *idpsClientGetter) GetIDPSClient() (rest.Interface, error) {
	return g.clientset, nil
}

type fakeController struct {
	*Controller
	mockController     *gomock.Controller
	mockSuricata       *suricatatest.MockInterface
	mockIDPSClient     *fakerest.RESTClient
	crdClient          *fakeversioned.Clientset
	crdInformerFactory crdinformers.SharedInformerFactory
}

func newFakeController(t *testing.T, objects []runtime.Object) *fakeController {
	controller := gomock.NewController(t)
	crdClient := fakeversioned.NewSimpleClientset(objects...)
	crdInformerFactory := crdinformers.NewSharedInformerFactory(crdClient, 0)
	signatureProviderInfoInformer := crdInformerFactory.TanzuCrd().V1alpha1().IDPSSignatureProviderInfos()

	mockIDPSClient := &fakerest.RESTClient{}
	mockSuricataDriver := suricatatest.NewMockInterface(controller)
	signatureController := NewController(signatureProviderInfoInformer, &idpsClientGetter{mockIDPSClient}, mockSuricataDriver)

	return &fakeController{
		Controller:         signatureController,
		mockController:     controller,
		mockSuricata:       mockSuricataDriver,
		mockIDPSClient:     mockIDPSClient,
		crdClient:          crdClient,
		crdInformerFactory: crdInformerFactory,
	}
}

func TestSignatureAdd(t *testing.T) {
	signature1 := "test-sig1"
	signature1Data := "alert http any any -> any any (msg: test-sig1; content: none; http_uri; sid:1;)"

	signature2 := "test-sig2"
	signature2Data := "alert http any any -> any any (msg: test-sig2; content: none; http_uri; sid:2;)"

	signature3 := "test-sig3"
	signature3Data := "alert http any any -> any any (msg: test-sig3; content: none; http_uri; sid:3;)"

	testCases := []struct {
		name            string
		signatureData   string
		signatureObject *tanzucrd.IDPSSignatureProviderInfo
	}{
		{
			name:          signature1,
			signatureData: signature1Data,
			signatureObject: &tanzucrd.IDPSSignatureProviderInfo{
				ObjectMeta: metav1.ObjectMeta{Name: signature1},
				SignatureBundle: tanzucrd.IDPSSignatureBundleInfo{
					Version:        1021,
					Sha256Checksum: bytesutil.Sha256Checksum([]byte(signature1Data)),
				},
			},
		},
		{
			name:          signature2,
			signatureData: signature2Data,
			signatureObject: &tanzucrd.IDPSSignatureProviderInfo{
				ObjectMeta: metav1.ObjectMeta{Name: signature2},
				SignatureBundle: tanzucrd.IDPSSignatureBundleInfo{
					Version:        1022,
					Sha256Checksum: bytesutil.Sha256Checksum([]byte(signature2Data)),
				},
			},
		},
		{
			name:          signature3,
			signatureData: signature3Data,
			signatureObject: &tanzucrd.IDPSSignatureProviderInfo{
				ObjectMeta: metav1.ObjectMeta{Name: signature3},
				SignatureBundle: tanzucrd.IDPSSignatureBundleInfo{
					Version:        1023,
					Sha256Checksum: bytesutil.Sha256Checksum([]byte(signature3Data)),
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeController(t, []runtime.Object{tt.signatureObject})

			c.mockIDPSClient.Client = fakerest.CreateHTTPClient(func(r *http.Request) (*http.Response, error) {
				w := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(tt.signatureData)),
				}
				return w, nil
			})

			defer c.mockController.Finish()

			stopCh := make(chan struct{})
			defer close(stopCh)

			c.crdInformerFactory.Start(stopCh)
			c.crdInformerFactory.WaitForCacheSync(stopCh)

			c.mockSuricata.EXPECT().LoadSignature(tt.name, []byte(tt.signatureData))
			require.NoError(t, c.syncSignatureProviderInfo(tt.name))
		})
	}
}
